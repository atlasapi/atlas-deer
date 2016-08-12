package org.atlasapi.hashing.extractors;

import com.metabroadcast.common.intl.Country;

public class CountryExtractor extends Extractor {

    private CountryExtractor() {
    }

    public static CountryExtractor create() {
        return new CountryExtractor();
    }

    @Override
    protected boolean isSupported(Object object) {
        return object instanceof Country;
    }

    @Override
    protected String extractValueInternal(Object object) {
        Country country = (Country) object;
        return country.getName() + country.code();
    }
}
