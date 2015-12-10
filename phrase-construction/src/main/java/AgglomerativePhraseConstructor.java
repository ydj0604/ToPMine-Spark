import org.apache.commons.lang.mutable.MutableLong;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by Jin on 11/18/2015.
 */
public class AgglomerativePhraseConstructor implements Serializable {
    private static final double SIGNIFICANCE_SCORE_THRESHOLD = 5.0;
    private final PhraseDictionary phraseDictionary;
    private long totalNumWordsAndPhrases;
    private final Set<String> stopWordsSet;

    private class AdjacentPhrasesNode { // linked list node
        private  String leftPhrase;
        private  String rightPhrase;
        private double significanceScore;

        public AdjacentPhrasesNode leftNode;
        public AdjacentPhrasesNode rightNode;

        public AdjacentPhrasesNode(String leftPhrase, String rightPhrase) throws PhraseConstructionException {
            if(!Utility.isValidPhrase(leftPhrase) || !Utility.isValidPhrase(rightPhrase)) {
                throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument(s) in AdjacentPhrasesNode");
            }

            this.leftPhrase = leftPhrase;
            this.rightPhrase = rightPhrase;
            this.significanceScore = calculateSignificanceScore(leftPhrase, rightPhrase);
            this.leftNode = null;
            this.rightNode = null;
        }

        public void updateLeftPhrase(String newPhrase) throws PhraseConstructionException {
            if(!Utility.isValidPhrase(newPhrase)) {
                throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument in updateLeftPhrase");
            }
            this.leftPhrase = newPhrase;
            this.significanceScore = calculateSignificanceScore(leftPhrase, rightPhrase);
        }

        public void updateRightPhrase(String newPhrase) throws PhraseConstructionException {
            if(!Utility.isValidPhrase(newPhrase)) {
                throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument in updateRightPhrase");
            }
            this.rightPhrase = newPhrase;
            this.significanceScore = calculateSignificanceScore(leftPhrase, rightPhrase);
        }

        // compares significance score: positive if this is bigger than other
        public int compareTo(AdjacentPhrasesNode other) {
            if(this.significanceScore > other.significanceScore) {
                return 1;
            } else if(this.significanceScore == other.significanceScore) {
                return 0;
            } else {
                return -1;
            }
        }

        public String getLeftPhrase() {
            return leftPhrase;
        }

        public String getRightPhrase() {
            return rightPhrase;
        }

        public double getSignificanceScore() {
            return significanceScore;
        }

        @Override
        public String toString() {
            return leftPhrase + " " + rightPhrase;
        }
    }

    public AgglomerativePhraseConstructor(PhraseDictionary phraseDictionary, Set<String> stopWordsSet)
            throws PhraseConstructionException, MalformedURLException {

        if(phraseDictionary == null) {
            throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument(s) in AgglomerativePhraseConstructor");
        }

        this.phraseDictionary = phraseDictionary;
        this.stopWordsSet = stopWordsSet;
        this.totalNumWordsAndPhrases = phraseDictionary.getSize();
    }

    public long getTotalNumWordsAndPhrases() {
        return totalNumWordsAndPhrases;
    }

    public double calculateSignificanceScore(String phrase1, String phrase2) throws PhraseConstructionException {
        if(!Utility.isValidPhrase(phrase1) || !Utility.isValidPhrase(phrase2)) {
            throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument(s) in calculateSignificanceScore");
        }

        // TODO: Hmmmm..!!

        double actualFreqOfCombined = (double) phraseDictionary.getCountOfPhrase(phrase1 + " " + phrase2);
        double expectedFreqOfCombined = ((double)phraseDictionary.getCountOfPhrase(phrase1)/(double) totalNumWordsAndPhrases)
                * ((double)phraseDictionary.getCountOfPhrase(phrase2)/(double) totalNumWordsAndPhrases)
                * totalNumWordsAndPhrases;

        double numerator = actualFreqOfCombined - expectedFreqOfCombined,
                denominator = Math.max(actualFreqOfCombined, expectedFreqOfCombined)==0.0? 1.0 : Math.sqrt(Math.max(actualFreqOfCombined, expectedFreqOfCombined));

        return numerator / denominator;
    }

    public List<String> splitSentenceIntoPhrases(String sentence) throws PhraseConstructionException {
        String[] words = Utility.tokenize(sentence, stopWordsSet);

        // start a splitting process
        List<String> resultPhrases = new ArrayList<>();

        // return immediately if there is no need for split
        if(words.length == 0) {
            return resultPhrases;
        } else if(words.length == 1) {
            resultPhrases.add(words[0]);
            return resultPhrases;
        }

        // construct a doubly-linked list of AdjacentPhrasesNode and put every node into priority queue
        PriorityQueue<AdjacentPhrasesNode> nodeQueue = new PriorityQueue<>(words.length-1, (n1, n2)->n2.compareTo(n1));
        AdjacentPhrasesNode head = null, tail = null;

        for(int i=0; i<words.length-1; i++) {
            AdjacentPhrasesNode newNode = new AdjacentPhrasesNode(words[i], words[i+1]);
            if(i==0) {
                head = newNode;
            } else {
                // at this point, head and tail must not be null
                tail.rightNode = newNode;
                newNode.leftNode = tail;
            }
            tail = newNode;
            nodeQueue.add(newNode);
        }

        // agglomerative merging
        while(nodeQueue.size() > 1) { // this loop always leaves the last pair without attempting to merge
            final AdjacentPhrasesNode mergeCandidate = nodeQueue.poll();
            if(mergeCandidate.getSignificanceScore() >= SIGNIFICANCE_SCORE_THRESHOLD) {
                if(mergeCandidate.leftNode != null) {
                    nodeQueue.remove(mergeCandidate.leftNode); // TODO: removing from built-in PQ is linear. Need improvement?
                    mergeCandidate.leftNode.updateRightPhrase(mergeCandidate.toString()); // update merged phrase
                    nodeQueue.add(mergeCandidate.leftNode); // add the updated left node to queue
                    mergeCandidate.leftNode.rightNode = mergeCandidate.rightNode; // update linked list structure
                }

                if(mergeCandidate.rightNode != null) {
                    nodeQueue.remove(mergeCandidate.rightNode); // TODO: removing from built-in PQ is linear. Need improvement?
                    mergeCandidate.rightNode.updateLeftPhrase(mergeCandidate.toString()); // update merged phrase
                    nodeQueue.add(mergeCandidate.rightNode); // add the updated right node to queue
                    mergeCandidate.rightNode.leftNode = mergeCandidate.leftNode; // update linked list structure
                }

                if(mergeCandidate == head) {
                    head = mergeCandidate.rightNode; // update head
                }
            } else {
                break;
            }
        }

        // decide whether to merge the last pair or not
        if(nodeQueue.size() == 1) {
            AdjacentPhrasesNode lastNode = nodeQueue.poll();
            if(lastNode.getSignificanceScore() >= SIGNIFICANCE_SCORE_THRESHOLD) {
                resultPhrases.add(lastNode.toString());
                return resultPhrases;
            } else {
                resultPhrases.add(lastNode.getLeftPhrase());
                resultPhrases.add(lastNode.getRightPhrase());
                return resultPhrases;
            }
        }

        // at this point, linked list contains merge result
        AdjacentPhrasesNode curr = head;
        while(curr != null) {
            if(curr.rightNode == null) { // last node in the linked list
                resultPhrases.add(curr.getLeftPhrase());
                resultPhrases.add(curr.getRightPhrase());
            } else {
                resultPhrases.add(curr.getLeftPhrase());
            }
            curr = curr.rightNode;
        }

        return resultPhrases;
    }

    public String convertSentenceToSparseVecStr(String sentence) throws PhraseConstructionException {
        Map<String, MutableLong> phraseToCount = new HashMap<>();

        // split sentence
        List<String> phrases = splitSentenceIntoPhrases(sentence); // resultant phrases are valid

        // count phrases
        for(int i=0; i<phrases.size(); i++) {
            String currPhrase = phrases.get(i);
            phraseToCount.computeIfAbsent(currPhrase, k->new MutableLong(0L)).increment();
        }

        // construct resultant string that represents sparse vector
        StringBuilder stringBuilder = new StringBuilder();
        for(Map.Entry<String, MutableLong> entry : phraseToCount.entrySet()) {
            int currPhraseIdx = phraseDictionary.getIdxOfPhrase(entry.getKey());
            long currCount = entry.getValue().longValue();
            stringBuilder.append(String.format("%d:%d,", currPhraseIdx, currCount));
        }

        String result = stringBuilder.toString();
        if(result.length() == 0) {
            return "";
        }

        // remove the last comma
        result = result.substring(0, result.length()-1);

        return result; // INDEX:COUNT,INDEX:COUNT, ...
    }

    public String convertDocumentToSparseVecStr(String doc) throws PhraseConstructionException {
        Map<String, MutableLong> phraseToCount = new HashMap<>();

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
            List<String> phraseSegments = splitSentenceIntoPhrases(currSentence);
            for(String phrase : phraseSegments) {
                phraseToCount.computeIfAbsent(phrase, k->new MutableLong(0L)).increment();
            }
        }

        // compile the output
        StringBuilder stringBuilder = new StringBuilder();
        for(Map.Entry<String, MutableLong> entry : phraseToCount.entrySet()) {
            int currPhraseIdx = phraseDictionary.getIdxOfPhrase(entry.getKey());
            long currCount = entry.getValue().longValue();
            stringBuilder.append(String.format("%d:%d,", currPhraseIdx, currCount));
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
