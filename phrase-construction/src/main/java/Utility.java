import org.apache.commons.lang.mutable.MutableInt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jin on 11/26/2015.
 */
public final class Utility {
    public static boolean isValidPhrase(String phrase) {
        // TODO: check special chars
        return phrase!=null && phrase.length()!=0 && phrase.charAt(0)!=' ' && phrase.charAt(phrase.length()-1)!=' ';
    }

    public static String[] tokenize(String line) throws PhraseConstructionException {
        if (line == null || line.length() == 0) {
            throw new PhraseConstructionException("Utility: invalid argument in tokenize");
        }

        if (line.charAt(line.length() - 1) == '.') { // remove periods
            line = line.substring(0, line.length() - 1);
        }

        // use lower case only
        String[] rawWords = line.toLowerCase().split(" "); // TODO: Assumption: words are separated only by a single white space

        // filter the array of words
        List<String> wordList = new ArrayList<>();
        for (String word : rawWords) {
            if (word == null || word.length() == 0) {
                continue;
            }
            word = word.trim();
            word = word.replaceAll("[^\\w]", ""); // remove anything that is not a word char (a-z, 0-9, _)
            if (word.length() > 0) {
                // TODO: get the ROOT WORD and insert ?
                wordList.add(word);
            }
        }
        String[] words = wordList.stream().toArray(String[]::new); // convert it back to an array
        return words;
    }

    public static String convertPhraseListOfDocumentToSparseVec(List<String> phraseList, PhraseDictionary phraseDictionary)
            throws PhraseConstructionException {
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
        builder.deleteCharAt(builder.length() - 1); // remove the last comma
        return builder.toString();
        // ex) PHRASE_INDEX:COUNT,PHRASE_INDEX:COUNT,... (for doc1)
        //     PHRASE_INDEX:COUNT,PHRASE_INDEX:COUNT,... (for doc2)
    }

    public static void writeToFile(Object o, String filePath) throws IOException {
        File outputFile = new File(filePath);
        try(FileWriter fileWriter = new FileWriter(outputFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            bufferedWriter.write(o.toString());
        }
    }
}