import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Created by Jin on 11/18/2015.
 */
public class Main {
    public static final String OUTPUT_FILE_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/output.txt";
    public static final String CORPUS_FILE_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dblp_titles.txt";
    public static final String DICT_FILE_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dict.txt";
    public static final String OUTPUT_DIR_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dblp_output/";

    public static void main(String[] args) throws IOException, PhraseConstructionException {
        System.out.println("Hello Spark");
        LocalDateTime startTime = LocalDateTime.now();

        // prepare conf
        SparkConf conf = new SparkConf().setAppName("Phrase Construction");

        try(JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            PhraseDictionary phraseDictionary = SparkJob.constructPhraseDictionary(javaSparkContext, new PhraseMiner(), CORPUS_FILE_PATH);
            Utility.writeToFile(phraseDictionary, DICT_FILE_PATH);
            SparkJob.agglomerativeMerge(javaSparkContext, new AgglomerativePhraseConstructor(phraseDictionary), CORPUS_FILE_PATH, OUTPUT_DIR_PATH);
        }

        long elapsedTimeInSeconds = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - startTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println("Elpased Time: " + elapsedTimeInSeconds + " seconds");
    }
}
