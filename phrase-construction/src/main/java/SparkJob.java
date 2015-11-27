import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jin on 11/18/2015.
 */
public final class SparkJob {
    public static final int MAX_LEN_PHRASE = 20;

    // Helper Methods
    private static Map<Long, List<Integer>> mergeIdxMaps(Map<Long, List<Integer>> map1, Map<Long, List<Integer>> map2) {
        Map<Long, List<Integer>> merged = new HashMap<>();
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

    // PART I : Phrase Mining
    public static PhraseDictionary constructPhraseDictionary(JavaSparkContext javaSparkContext, PhraseMiner phraseMiner, String corpusFilePath)
            throws PhraseConstructionException {

        // prepare RDD
        JavaRDD<String> corpus = javaSparkContext.textFile(corpusFilePath); // TODO: assumption: each line is an one-sentence document without a period
        JavaPairRDD<String, Long> sentenceIdx = corpus.zipWithIndex();
        JavaPairRDD<Long, String> idxSentence = sentenceIdx.mapToPair(tuple -> tuple.swap());
        JavaPairRDD<Long, String[]> idxSentenceTokens = idxSentence.mapToPair(tuple -> new Tuple2<>(tuple._1, Utility.tokenize(tuple._2)));

        // PART I : phrase mining
        // variables to be used across all iterations
        Map<Long, List<Integer>> sentenceIdToIndicesOfPhrasePrevLength = null;
        Map<String, MutableLong> phraseToCount = new HashMap<>();

        // for each length n of phrases
        for(int n=1; n<MAX_LEN_PHRASE; n++) {
            final Integer N = n;
            final Map<Long, List<Integer>> sentenceIdToIndicesOfPhrasePrevLengthTemp = sentenceIdToIndicesOfPhrasePrevLength;
            final Map<String, MutableLong> phraseToCountTemp = phraseToCount;

            // get indices of all length-N (=curr length) phrases
            JavaRDD<Map<Long, List<Integer>>> sentenceIdToIndicesOfPhraseCurrLength =
                    idxSentenceTokens.map(tuple -> phraseMiner.findIndicesOfPhraseLengthN_ForSentenceIdS(tuple._2, tuple._1, N, sentenceIdToIndicesOfPhrasePrevLengthTemp, phraseToCountTemp));
            // TODO: filter sentence with an empty list
            final Map<Long, List<Integer>> mergedSentenceIdToIndicesOfPhraseCurrLength = sentenceIdToIndicesOfPhraseCurrLength.reduce((map1, map2) -> mergeIdxMaps(map1, map2));

            // count phrases of length-N
            JavaRDD<Map<String, MutableLong>> phraseToCountCurr =
                    idxSentenceTokens.map(tuple -> phraseMiner.countPhraseOfLengthN_InSentenceIdS(tuple._2, tuple._1, N, mergedSentenceIdToIndicesOfPhraseCurrLength));
            Map<String, MutableLong> mergedPhraseToCountCurr = phraseToCountCurr.reduce((map1, map2) -> mergePhraseCountMaps(map1, map2));
            phraseToCount = mergePhraseCountMaps(phraseToCount, mergedPhraseToCountCurr);

            // update sentenceIdToIndicesOfPhrase
            sentenceIdToIndicesOfPhrasePrevLength = mergedSentenceIdToIndicesOfPhraseCurrLength;
        }

        return new PhraseDictionary(phraseToCount);
    }

    // PART II : Agglomerative Merging
    public static void agglomerativeMerge(JavaSparkContext javaSparkContext, AgglomerativePhraseConstructor aggPhraseConstructor, PhraseDictionary phraseDictionary,
                           String corpusFilePath, String outputDirPath) throws PhraseConstructionException {

        JavaRDD<String> corpus = javaSparkContext.textFile(corpusFilePath); // TODO: assumption: each line is an one-sentence document without a period
        //JavaRDD<String> bagOfPhrasesSparseVecs = corpus.map(line -> convertPhraseListOfDocumentToSparseVec(aggPhraseConstructor.splitSentenceIntoPhrases(line)));
        JavaRDD<List<String>> bagOfPhrasesSparseVecs = corpus.map(line -> aggPhraseConstructor.splitSentenceIntoPhrases(line));

        // output to a file since modeling lda is done in python
        // toString() is called on each RDD to write: the number of output files = the number of RDDs
        bagOfPhrasesSparseVecs.saveAsTextFile(outputDirPath);

        // TODO: testing - output phrase dictionary to stdout
        System.out.println(phraseDictionary);
    }
}
