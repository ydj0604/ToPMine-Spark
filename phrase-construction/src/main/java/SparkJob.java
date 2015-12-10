import org.apache.commons.collections.Bag;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.*;
import java.util.*;

/**
 * Created by Jin on 11/18/2015.
 */
public final class SparkJob {
    private static final int MAX_LEN_PHRASE = 10;
    private static final int NUM_TOPICS = 10;
    private static final int NUM_WORDS_TO_SHOW_FOR_EACH_TOPIC = 25;
    private static final double LDA_ALPHA = 50/NUM_TOPICS + 1;
    private static final double LDA_BETA = 1.1;

    // Helper Methods
    private static Map<Long, List<Integer>> mergeIdxMaps(Map<Long, List<Integer>> map1, Map<Long, List<Integer>> map2) {
        Map<Long, List<Integer>> merged = new HashMap<>(); // TODO: new object necessary ?
        merged.putAll(map1);
        merged.putAll(map2);
        return merged;
    }

    private static Map<String, MutableLong> mergePhraseCountMaps(Map<String, MutableLong> map1, Map<String, MutableLong> map2) {
        Map<String, MutableLong> merged = new HashMap<>();
        merged.putAll(map1);

        for(Map.Entry<String, MutableLong> entry : map2.entrySet()) {
            merged.computeIfAbsent(entry.getKey(), k -> new MutableLong(0)).add(entry.getValue());
        }
        return merged;
    }

    private static Vector convertToSparseVecObj(String sparseVecStr, long totalNumWordsAndPhrases) {
        String[] elements = sparseVecStr.split(",");
        int[] indices = new int[elements.length];
        double[] values = new double[elements.length];

        for(int i=0; i<elements.length; i++) {
            String element = elements[i];
            int idx = Integer.parseInt(element.split(":")[0]);
            double count = Double.parseDouble(element.split(":")[1]);
            indices[i] = idx;
            values[i] = count;
        }

        return Vectors.sparse((int)totalNumWordsAndPhrases, indices, values);
    }

    // SparkJob.runBagOfWordsLDA(javaSparkContext, CORPUS_FILE_PATH, OUTPUT_FILE_PATH + "_words", DICT_FILE_PATH + "_words", STOP_WORDS_FILE_PATH);
    public static void runBagOfWordsLDA(JavaSparkContext javaSparkContext, String corpusFilePath, String outputFilePath,
                                        String dictFilePath, String stopWordsFilePath) throws PhraseConstructionException, IOException {

        JavaRDD<String> corpus = javaSparkContext.textFile(corpusFilePath); // TODO: assumption: each line is a document; need to split into sentences
        Set<String> stopWordsSet = Utility.buildStopWordsSet(stopWordsFilePath);
        PhraseDictionary phraseDictionary = constructPhraseDictionary(javaSparkContext, new PhraseMiner(stopWordsSet), corpusFilePath);
        Utility.writeToFile(phraseDictionary, dictFilePath);
        BagOfWordsConstructor bagOfWordsConstructor = new BagOfWordsConstructor(phraseDictionary, stopWordsSet);
        JavaRDD<String> bagOfWordsSparseVecStrs = corpus.map(line -> bagOfWordsConstructor.convertDocumentToSparseVecStr(line));
        bagOfWordsSparseVecStrs = bagOfWordsSparseVecStrs.filter(line -> line.length()>0); // filter out empty sparse vec str
        JavaRDD<Vector> bagOfWordsSparseVecObjs = bagOfWordsSparseVecStrs.map(line -> convertToSparseVecObj(line, phraseDictionary.getSize()));
        ldaModeling(bagOfWordsSparseVecObjs, outputFilePath, phraseDictionary);
    }

    // main method
    public static void runPhraseMining(JavaSparkContext javaSparkContext, String corpusFilePath, String outputFilePath,
                    String dictFilePath, String stopWordsFilePath) throws PhraseConstructionException, IOException {

        // build stopwords set
        Set<String> stopWordsSet = Utility.buildStopWordsSet(stopWordsFilePath);

        // build phrase dict
        PhraseDictionary phraseDictionary = constructPhraseDictionary(javaSparkContext, new PhraseMiner(stopWordsSet), corpusFilePath);

        // write phrase dictionary
        Utility.writeToFile(phraseDictionary, dictFilePath); // write phrase dict to file

        // bag of phrases construction
        JavaRDD<Vector> vectorizedCorpus = agglomerativeMergeOnEntireCorpus(javaSparkContext, new AgglomerativePhraseConstructor(phraseDictionary, stopWordsSet), corpusFilePath);

        // lda modeling
        ldaModeling(vectorizedCorpus, outputFilePath, phraseDictionary);
    }

    // PART I : Phrase Mining
    private static PhraseDictionary constructPhraseDictionary(JavaSparkContext javaSparkContext, PhraseMiner phraseMiner, String corpusFilePath)
            throws PhraseConstructionException {

        // prepare RDD
        JavaRDD<String> corpus = javaSparkContext.textFile(corpusFilePath);
        JavaRDD<String> corpusWithEachLineSentenceUnfiltered = corpus.flatMap(line -> Arrays.asList(line.split("\\."))); // TODO: assumption: each line is a document; need to split into sentences
        JavaRDD<String> corpusWithEachLineSentence = corpusWithEachLineSentenceUnfiltered.filter(line -> line.length() > 0); // remove empty sentences
        JavaPairRDD<Long, String> idxAndSentence = corpusWithEachLineSentence.zipWithIndex().mapToPair(tuple -> tuple.swap());
        idxAndSentence.cache(); // persist for iterative process

        // PART I : phrase mining
        // variables to be used across all iterations
        Map<Long, List<Integer>> sentenceIdxToIndicesOfPhrasePrevLength = null;
        Map<String, MutableLong> phraseToCount = new HashMap<>();

        // for each length n of phrases
        for(int n=1; n<MAX_LEN_PHRASE; n++) {
            final Integer N = n;
            final Map<Long, List<Integer>> sentenceIdxToIndicesOfPhrasePrevLengthTemp = sentenceIdxToIndicesOfPhrasePrevLength;
            final Map<String, MutableLong> phraseToCountTemp = phraseToCount;

            // get indices of all length-N (=curr length) phrases
            JavaRDD<Map<Long, List<Integer>>> sentenceIdxToIndicesOfCandidatePhraseCurrLength =
                    idxAndSentence.map(tuple -> phraseMiner.findIndicesOfCandidatePhraseLengthN_ForSentenceIdxS(tuple._2(), tuple._1(), N, sentenceIdxToIndicesOfPhrasePrevLengthTemp, phraseToCountTemp));
            final Map<Long, List<Integer>> mergedSentenceIdxToIndicesOfCandidatePhraseCurrLength = sentenceIdxToIndicesOfCandidatePhraseCurrLength.reduce((map1, map2) -> mergeIdxMaps(map1, map2));

            // filter out sentences with an empty candidate list
            idxAndSentence = idxAndSentence.filter(tuple -> mergedSentenceIdxToIndicesOfCandidatePhraseCurrLength.get(tuple._1()).size() > 0);
            if(idxAndSentence.count() == 0) {
                break;
            }

            // count phrases of length-N
            JavaRDD<Map<String, MutableLong>> phraseToCountCurr =
                    idxAndSentence.map(tuple -> phraseMiner.countPhraseOfLengthN_InSentenceIdxS(tuple._2(), tuple._1(), N, mergedSentenceIdxToIndicesOfCandidatePhraseCurrLength));
            Map<String, MutableLong> mergedPhraseToCountCurr = phraseToCountCurr.reduce((map1, map2) -> mergePhraseCountMaps(map1, map2));
            phraseToCount = mergePhraseCountMaps(phraseToCount, mergedPhraseToCountCurr);

            // update sentenceIdToIndicesOfPhrase
            sentenceIdxToIndicesOfPhrasePrevLength = mergedSentenceIdxToIndicesOfCandidatePhraseCurrLength;
        }

        return new PhraseDictionary(phraseToCount);
    }

    // PART II : Agglomerative Merging
    private static JavaRDD<Vector> agglomerativeMergeOnEntireCorpus(JavaSparkContext javaSparkContext, AgglomerativePhraseConstructor aggPhraseConstructor,
        String corpusFilePath) throws PhraseConstructionException {

        // TF
        JavaRDD<String> corpus = javaSparkContext.textFile(corpusFilePath); // TODO: assumption: each line is a document; need to split into sentences
        JavaRDD<String> bagOfPhrasesSparseVecStrs = corpus.map(line -> aggPhraseConstructor.convertDocumentToSparseVecStr(line));
        bagOfPhrasesSparseVecStrs = bagOfPhrasesSparseVecStrs.filter(line -> line.length()>0); // filter out empty sparse vec str
        JavaRDD<Vector> bagOfPhrasesSparseVecObjs = bagOfPhrasesSparseVecStrs.map(str -> convertToSparseVecObj(str, aggPhraseConstructor.getTotalNumWordsAndPhrases()));

        // IDF
        IDFModel idfModel = new IDF().fit(bagOfPhrasesSparseVecObjs);
        JavaRDD<Vector> bagOfPhrasesSparseVecObjsWithIdf = idfModel.transform(bagOfPhrasesSparseVecObjs);

        return bagOfPhrasesSparseVecObjsWithIdf;
    }

    // PART III : LDA modeling
    private static void ldaModeling(JavaRDD<Vector> vectorizedCorpus, String outputFilePath, PhraseDictionary phraseDictionary) throws IOException {
        // prepare corpus
        JavaPairRDD<Long, Vector> vectorizedCorpusWithIdx = vectorizedCorpus.zipWithIndex().mapToPair(tuple -> tuple.swap());
        vectorizedCorpusWithIdx.cache(); // persist in memory for iterative process of lda

        // run lda
        DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(NUM_TOPICS).setAlpha(LDA_ALPHA).setBeta(LDA_BETA).run(vectorizedCorpusWithIdx);
        Matrix topics = ldaModel.topicsMatrix();

        // write output
        try(FileWriter fileWriter = new FileWriter(outputFilePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            for(int topic=0; topic<NUM_TOPICS; topic++) {
                bufferedWriter.write("Topic " + topic + ":" + System.getProperty("line.separator"));

                List<Pair<String, Double>> phraseAndProbList = new ArrayList<>();
                for(int word = 0; word < ldaModel.vocabSize(); word++) {
                    phraseAndProbList.add(Pair.of(phraseDictionary.getPhrase(word), topics.apply(word, topic))); // TODO: what is apply return value?
                }
                Collections.sort(phraseAndProbList, (p1, p2) -> p2.getRight().compareTo(p1.getRight()));

                for(int i=0; i<NUM_WORDS_TO_SHOW_FOR_EACH_TOPIC && i<ldaModel.vocabSize(); i++) {
                    Pair<String, Double> curr = phraseAndProbList.get(i);
                    bufferedWriter.write(String.format("  %s:%.2f%s", curr.getLeft(), curr.getRight(), System.getProperty("line.separator")));
                }

                bufferedWriter.write(System.getProperty("line.separator"));
            }

            // write other info
            bufferedWriter.write(String.format("K: %d, Log-Likelihood: %.3f, Alpha: %.3f, Beta: %.3f", NUM_TOPICS, ldaModel.logLikelihood(), LDA_ALPHA, LDA_BETA));
        }
    }
}
