import org.apache.commons.lang.mutable.MutableLong;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Jin on 12/9/2015.
 */
public class BagOfWordsConstructor {
    private final PhraseDictionary phraseDictionary;
    private final Set<String> stopWordsSet;

    public BagOfWordsConstructor(PhraseDictionary phraseDictionary, Set<String> stopWordsSet) {
        this.phraseDictionary = phraseDictionary; // also contains words
        this.stopWordsSet = stopWordsSet;
    }

    public String convertDocumentToSparseVecStr(String doc) throws PhraseConstructionException {
        Map<String, MutableLong> wordToCount = new HashMap<>();

        // split sentences by period
        String[] sentences = doc.split("\\.");
        if(sentences.length < 1) {
            return "";
        }

        // process each sentence
        for(String currSentence: sentences) {
            if(currSentence.length() == 0) {
                continue; // ignore empty sentence
            }
            String[] wordSegments = Utility.tokenize(currSentence, stopWordsSet);
            for(String word : wordSegments) {
                if(word.length() == 0) {
                    continue;
                }
                wordToCount.computeIfAbsent(word, k -> new MutableLong(0L)).increment();
            }
        }

        // compile the output
        StringBuilder stringBuilder = new StringBuilder();
        for(Map.Entry<String, MutableLong> entry : wordToCount.entrySet()) {
            int currWordIdx = phraseDictionary.getIdxOfPhrase(entry.getKey());
            long currCount = entry.getValue().longValue();
            stringBuilder.append(String.format("%d:%d,", currWordIdx, currCount));
        }

        String result = stringBuilder.toString();
        if(result.length() == 0) {
            return "";
        }

        // remove the last comma
        result = result.substring(0, result.length()-1);

        return result; // INDEX:COUNT,INDEX:COUNT, ...
    }
}
