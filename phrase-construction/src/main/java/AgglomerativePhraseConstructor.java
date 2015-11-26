import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by Jin on 11/18/2015.
 */
public class AgglomerativePhraseConstructor implements Serializable {
    private static final double SIGNIFICANCE_SCORE_THRESHOLD = 2.0;
    private final PhraseDictionary phraseDictionary;
    private long totalNumWordsAndPhrases;

    private class AdjacentPhrasesNode { // linked list node
        private  String leftPhrase;
        private  String rightPhrase;
        private double significanceScore;

        public AdjacentPhrasesNode leftNode;
        public AdjacentPhrasesNode rightNode;

        public AdjacentPhrasesNode(String leftPhrase, String rightPhrase) throws PhraseConstructionException {
            if(!isValidPhrase(leftPhrase) || !isValidPhrase(rightPhrase)) {
                throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument(s) in AdjacentPhrasesNode");
            }

            this.leftPhrase = leftPhrase;
            this.rightPhrase = rightPhrase;
            this.significanceScore = calculateSignificanceScore(leftPhrase, rightPhrase);
            this.leftNode = null;
            this.rightNode = null;
        }

        public void updateLeftPhrase(String newPhrase) throws PhraseConstructionException {
            if(!isValidPhrase(newPhrase)) {
                throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument in updateLeftPhrase");
            }
            this.leftPhrase = newPhrase;
            this.significanceScore = calculateSignificanceScore(leftPhrase, rightPhrase);
        }

        public void updateRightPhrase(String newPhrase) throws PhraseConstructionException {
            if(!isValidPhrase(newPhrase)) {
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

    public AgglomerativePhraseConstructor(PhraseDictionary phraseDictionary) throws PhraseConstructionException, MalformedURLException {
        if(phraseDictionary == null) {
            throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument(s) in AgglomerativePhraseConstructor");
        }
        this.phraseDictionary = phraseDictionary;
        this.totalNumWordsAndPhrases = phraseDictionary.getSize();
    }

    public double calculateSignificanceScore(String phrase1, String phrase2) throws PhraseConstructionException {
        if(!isValidPhrase(phrase1) || !isValidPhrase(phrase2)) {
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

    public boolean isValidPhrase(String phrase) {
        // TODO: check special chars
        return phrase!=null && phrase.length()!=0 && phrase.charAt(0)!=' ' && phrase.charAt(phrase.length()-1)!=' ';
    }

    public List<String> splitSentenceIntoPhrases(String sentence) throws PhraseConstructionException {
        if(sentence == null) {
            throw new PhraseConstructionException("AgglomerativePhraseConstructor: invalid argument in splitSentenceIntoPhrases");
        }

        if(sentence.charAt(sentence.length()-1) == '.') { // remove periods
            sentence = sentence.substring(0, sentence.length()-1);
        }

        // use lower case only
        String[] rawWords = sentence.toLowerCase().split(" "); // TODO: Assumption: words are separated only by a single white space

        // filter the array of words
        List<String> wordList = new ArrayList<>();
        for(String word : rawWords) {
            if(word == null) {
                continue;
            }
            word = word.trim();

            if(word.length() > 0 && word.charAt(word.length()-1) < 'a' && word.charAt(word.length()-1) > 'z') {
                word = word.substring(0, word.length()-1); // remove the last special char
            }

            if(word.length()>0) {
                // TODO: get the root word and insert
                wordList.add(word);
            }
        }
        String[] words = wordList.stream().toArray(String[]::new); // convert it back to an array

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
        while(nodeQueue.size() > 1) { // this loop always leaves the last pair without merging
            AdjacentPhrasesNode mergeCandidate = nodeQueue.poll();
            if(mergeCandidate.significanceScore >= SIGNIFICANCE_SCORE_THRESHOLD) {
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

        // TODO: need to decide whether to merge the last pair or not
        // TODO: currently, it simply compares phrase counts
        if(nodeQueue.size() == 1) {
            AdjacentPhrasesNode lastNode = nodeQueue.poll();
            long mergedPhraseCount = phraseDictionary.getCountOfPhrase(lastNode.toString()).longValue(),
                    leftPhraseCount = phraseDictionary.getCountOfPhrase(lastNode.leftPhrase).longValue(),
                    rightPhraseCount = phraseDictionary.getCountOfPhrase(lastNode.rightPhrase).longValue();
            if(mergedPhraseCount >= Math.max(leftPhraseCount, rightPhraseCount)) {
                resultPhrases.add(lastNode.toString());
                return resultPhrases;
            } else {
                resultPhrases.add(lastNode.leftPhrase);
                resultPhrases.add(lastNode.rightPhrase);
                return resultPhrases;
            }
        }

        // at this point, linked list contains merge result
        AdjacentPhrasesNode curr = head;
        while(curr != null) {
            if(curr.rightNode == null) { // last node in the linked list
                resultPhrases.add(curr.leftPhrase);
                resultPhrases.add(curr.rightPhrase);
            } else {
                resultPhrases.add(curr.leftPhrase);
            }
            curr = curr.rightNode;
        }

        return resultPhrases;
    }
}
