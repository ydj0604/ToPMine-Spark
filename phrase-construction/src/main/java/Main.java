import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by Jin on 11/18/2015.
 */
public class Main {
    public static final String outputDirPath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/output/";
    public static final String corpusFilePath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/corpus.txt";
    public static final String dictFilePath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dict.txt";
    public static final long totalNumWords = 2000; // TODO: need to modify the code structure for this var

    public static void main(String[] args) throws IOException, PhraseConstructionException {
        System.out.println("Hello Spark");

        // prepare dependencies
        SparkConf conf = new SparkConf().setAppName("Phrase Construction");
        PhraseDictionary phraseDictionary = new PhraseDictionary(dictFilePath);
        AgglomerativePhraseConstructor agglomerativePhraseConstructor = new AgglomerativePhraseConstructor(phraseDictionary, totalNumWords);

        // run spark job through dependency injection
        SparkJob sparkJob = new SparkJob(phraseDictionary, agglomerativePhraseConstructor, corpusFilePath, outputDirPath);
        sparkJob.run(conf);
    }
}
