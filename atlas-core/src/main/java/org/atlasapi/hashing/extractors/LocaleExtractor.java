package org.atlasapi.hashing.extractors;

import java.util.Locale;

public class LocaleExtractor extends Extractor {

    private LocaleExtractor() {
    }

    public static LocaleExtractor create() {
        return new LocaleExtractor();
    }

    @Override
    protected boolean isSupported(Object object) {
        return object instanceof Locale;
    }

    @Override
    protected String extractValueInternal(Object object) {
        Locale locale = (Locale) object;
        return locale.getLanguage();
    }
}
