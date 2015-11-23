import org.apache.commons.lang.mutable.MutableInt;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jin on 11/18/2015.
 */
public class SparkJob implements  AutoCloseable {

    private final PhraseDictionary phraseDictionary; // dictionary used to get sparse vectors
    private final AgglomerativePhraseConstructor aggPhraseConstructor;
    private final JavaSparkContext javaSparkContext;
    private final String corpusFilePath;
    public final String outputDirPath;

    public SparkJob(PhraseDictionary phraseDictionary, AgglomerativePhraseConstructor agglomerativePhraseConstructor,
                    SparkConf conf, String corpusFilePath, String outputDirPath) {
        this.phraseDictionary = phraseDictionary;
        this.aggPhraseConstructor = agglomerativePhraseConstructor;
        this.javaSparkContext = new JavaSparkContext(conf);
        this.corpusFilePath = corpusFilePath;
        this.outputDirPath = outputDirPath;
    }

    private String convertPhraseListOfDocumentToSparseVec(List<String> phraseList) throws PhraseConstructionException {
        if(phraseList==null || phraseList.size()==0) {
            throw new PhraseConstructionException("SparkJob: invalid argument in convertPhraseListOfDocumentToSparseVec");
        }

        Map<String, MutableInt> phraseToCountMap = new HashMap<>();

        for(String phrase : phraseList) {
            if(phrase == null || phrase.equals("")) {
                continue;
            }
            phraseToCountMap.computeIfAbsent(phrase, k -> new MutableInt(0)).increment();
        }

        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String, MutableInt> entry : phraseToCountMap.entrySet()) {
            String phrase = entry.getKey();
            int phraseCount = entry.getValue().intValue();
            builder.append(phraseDictionary.getIdxOfPhrase(phrase) + ":" + phraseCount + ",");
        }
        builder.deleteCharAt(builder.length()-1); // remove the last comma
        return builder.toString();
    }

    public void run() throws PhraseConstructionException {
        JavaRDD<String> corpus = javaSparkContext.textFile(corpusFilePath); // TODO: assumption: each line is an one-sentence document without a period
        JavaRDD<String> bagOfPhrasesSparseVecs = corpus.map(line -> convertPhraseListOfDocumentToSparseVec(aggPhraseConstructor.splitSentenceIntoPhrases(line)));

        // output to a file since modeling lda is done in python
        // toString() is called on each RDD to write: the number of output files = the number of RDDs
        bagOfPhrasesSparseVecs.saveAsTextFile(outputDirPath);

        // print to stdout
        // TODO: if data is big, it will crash
        System.out.println(bagOfPhrasesSparseVecs.collect());
    }

    @Override
    public void close() {
        javaSparkContext.stop();
        javaSparkContext.close();
    }
}
