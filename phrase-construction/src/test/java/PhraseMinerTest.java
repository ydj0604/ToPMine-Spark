import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Jin on 11/22/2015.
 */
public class PhraseMinerTest {

    @BeforeClass
    public static void setUp() {

    }

    @Test
    public void test() throws PhraseConstructionException {
        PhraseMiner phraseMiner = new PhraseMiner(null);
        long sentenceIdx = 1;

        Map<Long, List<Integer>> actualCandidateIndices = phraseMiner.findIndicesOfCandidatePhraseLengthN_ForSentenceIdxS("Old Resolution Meets Modern SLS.", sentenceIdx, 1, null, null);
        Map<String, MutableLong> actualPhraseToCount = phraseMiner.countPhraseOfLengthN_InSentenceIdxS("Old Resolution Meets Modern SLS.", sentenceIdx, 1, actualCandidateIndices);

        Map<String, MutableLong> expectedPhraseToCount = new HashMap<>();
        expectedPhraseToCount.put("old", new MutableLong(1));
        expectedPhraseToCount.put("resolution", new MutableLong(1));
        expectedPhraseToCount.put("meets", new MutableLong(1));
        expectedPhraseToCount.put("modern", new MutableLong(1));
        expectedPhraseToCount.put("sls", new MutableLong(1));

        Assert.assertArrayEquals(new Integer[]{0, 1, 2, 3, 4}, actualCandidateIndices.get(sentenceIdx).toArray());
        Assert.assertEquals(expectedPhraseToCount, actualPhraseToCount);
    }
}
