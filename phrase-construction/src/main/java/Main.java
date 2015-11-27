import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by Jin on 11/18/2015.
 */
public class Main {
    public static final String OUTPUT_DIR_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dblp_output/";
    public static final String CORPUS_FILE_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/dblp_titles.txt";
    public static final String DICT_FILE_PATH = "/home/dongjinyu/workspace/ToPMine/phrase-construction/src/resources/phrases_count.txt";

    public static void main(String[] args) throws IOException, PhraseConstructionException {
        System.out.println("Hello Spark");

        // prepare conf
        SparkConf conf = new SparkConf().setAppName("Phrase Construction");

        try(JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            PhraseDictionary phraseDictionary = SparkJob.constructPhraseDictionary(javaSparkContext, new PhraseMiner(), CORPUS_FILE_PATH);
            System.out.println(phraseDictionary); // TODO: testing
            //SparkJob.agglomerativeMerge(javaSparkContext, new AgglomerativePhraseConstructor(phraseDictionary), phraseDictionary, CORPUS_FILE_PATH, OUTPUT_DIR_PATH);
        }
    }
}
