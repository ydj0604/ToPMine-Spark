import org.apache.commons.lang.mutable.MutableLong;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Jin on 11/25/2015.
 */
public class PhraseMiner implements Serializable {
    private static final int PHRASE_MIN_FREQ = 10;
    private final Set<String> stopWordsSet;

    public PhraseMiner(Set<String> stopWordsSet) {
        this.stopWordsSet = stopWordsSet;
    }

    public Map<Long, List<Integer>> findIndicesOfCandidatePhraseLengthN_ForSentenceIdxS(String sentence, long S, int N,
                                                                                        Map<Long, List<Integer>> sentenceIdToIndicesOfPhraseLengthN_MinusOne,
                                                                                        Map<String, MutableLong> phraseToCount) throws PhraseConstructionException {

        Map<Long, List<Integer>> sentenceIdxToIndicesOfCandidatePhraseLengthN = new HashMap<>();
        sentenceIdxToIndicesOfCandidatePhraseLengthN.put(S, new ArrayList<>());

        String[] sentenceTokens = Utility.tokenize(sentence, stopWordsSet);
        if(sentenceTokens.length == 0) {
            return sentenceIdxToIndicesOfCandidatePhraseLengthN;
        }

        // special cases
        if(N < 1 || S < 0) {
            throw new PhraseConstructionException("PhraseMiner: invalid input in findIndicesOfCandidatePhraseLengthN_ForSentenceIdxS");
        } else if(N == 1) {
            for(int i=0; i<sentenceTokens.length; i++) {
                sentenceIdxToIndicesOfCandidatePhraseLengthN.get(S).add(i);
            }
            return sentenceIdxToIndicesOfCandidatePhraseLengthN;
        }

        // at this point, N > 1, input maps should be non-null
        // for each starting idx of length N-1 phrase
        for(Integer idx : sentenceIdToIndicesOfPhraseLengthN_MinusOne.get(S)) {
            if(idx + N-1 > sentenceTokens.length) {
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
                sentenceIdxToIndicesOfCandidatePhraseLengthN.get(S).add(idx);
            }
        }

        return sentenceIdxToIndicesOfCandidatePhraseLengthN;
    }

    public Map<String, MutableLong> countPhraseOfLengthN_InSentenceIdxS(String sentence, long S, int N,
                                                                        Map<Long, List<Integer>> sentenceIdxToIndicesOfPhraseLengthN) throws PhraseConstructionException {

        String[] sentenceTokens = Utility.tokenize(sentence, stopWordsSet);
        if(sentenceTokens.length == 0) {
            return new HashMap<>();
        }

        Map<String, MutableLong> phraseToMutableCount = new HashMap<>();
        List<Integer> indicesOfPhraseLengthN = sentenceIdxToIndicesOfPhraseLengthN.get(S);

        for(Integer idx : indicesOfPhraseLengthN) {
            if(N==1 || indicesOfPhraseLengthN.contains(idx+1)) { // TODO: should use Set<Integer> to speed up look-up ? / NEED N==1 for special case
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
}
