import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.mockito.Mockito;

import java.net.MalformedURLException;
import java.util.List;

/**
 * Created by Jin on 11/22/2015.
 */
public class AgglomerativePhraseConstructorTest {

    @BeforeClass
    public static void setUp() {

    }

    @Test
    public void splitSentenceIntoPhrasesTestNormalCase() throws PhraseConstructionException, MalformedURLException {

        // construct a mock dictionary
        PhraseDictionary dictMock = Mockito.mock(PhraseDictionary.class);
        Mockito.when(dictMock.getCountOfPhrase("markov blanket")).thenReturn(50L);
        Mockito.when(dictMock.getCountOfPhrase("markov")).thenReturn(70L);
        Mockito.when(dictMock.getCountOfPhrase("blanket")).thenReturn(65L);
        Mockito.when(dictMock.getCountOfPhrase("markov blanket feature selection")).thenReturn(10L);

        Mockito.when(dictMock.getCountOfPhrase("feature selection")).thenReturn(100L);
        Mockito.when(dictMock.getCountOfPhrase("feature")).thenReturn(300L);
        Mockito.when(dictMock.getCountOfPhrase("selection")).thenReturn(110L);

        Mockito.when(dictMock.getCountOfPhrase("support vector machines")).thenReturn(150L);
        Mockito.when(dictMock.getCountOfPhrase("support vector")).thenReturn(200L);
        Mockito.when(dictMock.getCountOfPhrase("vector machines")).thenReturn(155L);
        Mockito.when(dictMock.getCountOfPhrase("support")).thenReturn(190L);
        Mockito.when(dictMock.getCountOfPhrase("vector")).thenReturn(180L);
        Mockito.when(dictMock.getCountOfPhrase("machines")).thenReturn(250L);

        Mockito.when(dictMock.getCountOfPhrase("for support vector machines")).thenReturn(20L);
        Mockito.when(dictMock.getCountOfPhrase("for support vector")).thenReturn(33L);
        Mockito.when(dictMock.getCountOfPhrase("for support")).thenReturn(33L);
        Mockito.when(dictMock.getCountOfPhrase("for")).thenReturn(350L);

        Mockito.when(dictMock.getSize()).thenReturn(1000000L);

        // calculate actual result
        AgglomerativePhraseConstructor phraseConstructor = new AgglomerativePhraseConstructor(dictMock, null);
        List<String> actualResult = phraseConstructor.splitSentenceIntoPhrases("markov blanket feature selection for support vector machines.");

        // test validity
        System.out.println(actualResult);
        Assert.assertArrayEquals(new String[]{"markov blanket", "feature selection", "for", "support vector machines"}, actualResult.toArray());
        Assert.assertTrue(phraseConstructor.calculateSignificanceScore("zero", "score") == 0.0);
        Assert.assertTrue(phraseConstructor.calculateSignificanceScore("blanket", "feature") <= 0.0);
    }
}
