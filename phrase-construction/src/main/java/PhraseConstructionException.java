import java.io.Serializable;

/**
 * Created by Jin on 11/18/2015.
 */
public class PhraseConstructionException extends Exception implements Serializable {
    public PhraseConstructionException(String msg) {
        super(msg);
    }
}
