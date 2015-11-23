import org.apache.spark.SparkConf;

import java.io.IOException;

/**
 * Created by Jin on 11/18/2015.
 */
public class Main {
    public static final String outputDirPath = "output";
    public static final String corpusFilePath = "corpus";
    public static final String dictFilePath = "dict";
    public static final long totalNumWords = 2000; // TODO: need to modify the code structure for this var

    public static void main(String[] args) throws IOException, PhraseConstructionException {
        System.out.println("Hello Spark");

        SparkConf conf = new SparkConf().setAppName("Phrase Construction");
        PhraseDictionary phraseDictionary = new PhraseDictionary(dictFilePath);
        AgglomerativePhraseConstructor agglomerativePhraseConstructor = new AgglomerativePhraseConstructor(phraseDictionary, totalNumWords);

        try(SparkJob sparkJob = new SparkJob(phraseDictionary, agglomerativePhraseConstructor, conf, corpusFilePath, outputDirPath)) {
            sparkJob.run();
        }

    }
}
