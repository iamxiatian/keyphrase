package ruc.irm.extractor.keyword;

import java.util.HashMap;
import java.util.Map;

/**
 * 词语的全局权重，例如，根据DF的差异为不同词语赋予不同权重
 * <p/>
 * User: xiatian
 * Date: 4/2/13 2:44 PM
 */
public class PhraseWeight {
    private static Map<String, Float> phrases = new HashMap<String, Float>();

    public static final void setWeight(String phrase, float weight) {
        phrases.put(phrase, weight);
    }

    public static final void delete(String phrase) {
        phrases.remove(phrase);
    }

    public static float getWeight(String phrase, float defaultValue) {
        if (phrases.containsKey(phrase)) {
            return phrases.get(phrase);
        } else {
            return defaultValue;
        }
    }
}
