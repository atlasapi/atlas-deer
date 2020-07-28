package org.atlasapi.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import sherlock_client_shaded.org.apache.lucene.analysis.TokenStream;
import sherlock_client_shaded.org.apache.lucene.analysis.en.EnglishAnalyzer;
import sherlock_client_shaded.org.apache.lucene.analysis.standard.StandardAnalyzer;
import sherlock_client_shaded.org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * This class was kept around from pre-live-sherlock days because at the time we had not
 * implemented stop words. Look into removing this class when that happens.
 */
public class Strings {

    public static List<String> tokenize(String value, boolean filterStopWords) {
        List<String> tokensAsStrings = Lists.newArrayList();
        try {
            TokenStream tokens;
            if (filterStopWords) {
                tokens = new EnglishAnalyzer().tokenStream("", new StringReader(value));
            } else {
                tokens = new StandardAnalyzer()
                        .tokenStream("", new StringReader(value));
            }
            tokens.reset();
            while (tokens.incrementToken()) {
                CharTermAttribute token = tokens.getAttribute(CharTermAttribute.class);
                tokensAsStrings.add(token.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (tokensAsStrings.isEmpty() && filterStopWords) {
            return tokenize(value, false);
        } else {
            return tokensAsStrings;
        }
    }

    public static String flatten(String value) {
        return Joiner.on("")
                .join(tokenize(value, true))
                .replaceAll("[^a-zA-Z0-9]", "")
                .toLowerCase();
    }
}
