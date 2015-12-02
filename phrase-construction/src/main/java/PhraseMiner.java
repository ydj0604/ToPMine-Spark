import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jin on 11/25/2015.
 */
public class PhraseMiner implements Serializable {
    private static final int PHRASE_MIN_FREQ = 10;

    public Map<Long, List<Integer>> findIndicesOfPhraseLengthN_ForSentenceIdS(String sentence, long S, int N,
                                                               Map<Long, List<Integer>> sentenceIdToIndicesOfPhraseLengthN_MinusOne,
                                                               Map<String, MutableLong> phraseToCount) throws PhraseConstructionException {

        String[] sentenceTokens = Utility.tokenize(sentence);
        Map<Long, List<Integer>> sentenceIdToIndicesOfPhraseLengthN = new HashMap<>();
        sentenceIdToIndicesOfPhraseLengthN.put(S, new ArrayList<>());

        // special case
        if(N < 1 || S < 0) {
            throw new PhraseConstructionException("PhraseMiner: invalid input in findIndicesOfPhraseLengthN_ForSentenceIdS");
        } else if(N == 1) {
            for(int i=0; i<sentenceTokens.length; i++) {
                sentenceIdToIndicesOfPhraseLengthN.get(S).add(i);
            }
            return sentenceIdToIndicesOfPhraseLengthN;
        }

        // at this point, N > 1, input maps should be non-null
        // for each starting idx of length N-1 phrase
        for(Integer idx : sentenceIdToIndicesOfPhraseLengthN_MinusOne.get(S)) {
            if(idx + N-1 >= sentenceTokens.length) {
                continue; // if phrase of length N starting at idx can't exist, ignore
            }

            // construct a phrase of length N starting at idx
            StringBuilder phraseBuilder = new StringBuilder();
            for(int i=idx; i<idx+N-1; i++) {
                phraseBuilder.append(sentenceTokens[i]);
                if(i<idx+N-2) {
                    phraseBuilder.append(" ");
                }
            }
            String phrase = phraseBuilder.toString();

            // check if the phrase satisfies required min freq
            if(phraseToCount.containsKey(phrase) && phraseToCount.get(phrase).longValue() >= PHRASE_MIN_FREQ) {
                sentenceIdToIndicesOfPhraseLengthN.get(S).add(idx);
            }
        }

        return sentenceIdToIndicesOfPhraseLengthN;
    }

    public Map<String, MutableLong> countPhraseOfLengthN_InSentenceIdS(String sentence, long S, int N,
                                    Map<Long, List<Integer>> sentenceIdToIndicesOfPhraseLengthN) throws PhraseConstructionException {

        String[] sentenceTokens = Utility.tokenize(sentence);
        Map<String, MutableLong> phraseToMutableCount = new HashMap<>();
        List<Integer> indicesOfPhraseLengthN = sentenceIdToIndicesOfPhraseLengthN.get(S);

        for(Integer idx : indicesOfPhraseLengthN) {
            if(indicesOfPhraseLengthN.contains(idx+1)) { // TODO: should use Set<Integer> to speed up look-up ?
                StringBuilder phraseBuilder = new StringBuilder();
                for(int i=idx; i<idx+N; i++) {
                    phraseBuilder.append(sentenceTokens[i]);
                    if(i<idx+N-1) {
                        phraseBuilder.append(" ");
                    }
                }
                String phrase = phraseBuilder.toString();
                phraseToMutableCount.computeIfAbsent(phrase, k -> new MutableLong(0)).increment();
            }
        }

        return phraseToMutableCount;
    }

    // TODO: merge two methods
}
