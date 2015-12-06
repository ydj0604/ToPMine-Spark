import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Created by Jin on 11/18/2015.
 */
public class Main {
    public static final String CORPUS_FILE_PATH = "data\\dblp_titles.txt";
    public static final String OUTPUT_FILE_PATH = "output\\dblp_output.txt";
    public static final String DICT_FILE_PATH = "output\\dblp_dict.txt";
    public static final String STOP_WORDS_FILE_PATH = "data\\stopwords.txt";


    public static void main(String[] args) throws IOException, PhraseConstructionException {
        System.out.println("Hello Spark");
        LocalDateTime startTime = LocalDateTime.now();

        // prepare conf
        SparkConf conf = new SparkConf().setAppName("Phrase Construction");

        try(JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            SparkJob.runPhraseMining(javaSparkContext, CORPUS_FILE_PATH, OUTPUT_FILE_PATH, DICT_FILE_PATH, STOP_WORDS_FILE_PATH);
        }

        // measure time
        long elapsedTimeInSeconds = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC) - startTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println("Elpased Time: " + elapsedTimeInSeconds + " seconds");
    }
}
