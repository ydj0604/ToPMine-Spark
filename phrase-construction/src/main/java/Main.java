import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by Jin on 11/18/2015.
 */
public class Main {
    public static final String outputDirPath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dblp_output/";
    public static final String corpusFilePath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dblp_titles.txt";
    public static final String dictFilePath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/phrases_count.txt";
    public static final String wordNetDirPath = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/wordnet";

    public static void main(String[] args) throws IOException, PhraseConstructionException {
        System.out.println("Hello Spark");

        // prepare dependencies
        SparkConf conf = new SparkConf().setAppName("Phrase Construction");
        PhraseDictionary phraseDictionary = new PhraseDictionary(dictFilePath);
        AgglomerativePhraseConstructor agglomerativePhraseConstructor = new AgglomerativePhraseConstructor(phraseDictionary);

        // run spark job through dependency injection
        SparkJob sparkJob = new SparkJob(phraseDictionary, agglomerativePhraseConstructor, corpusFilePath, outputDirPath);
        sparkJob.run(conf);
    }
}
