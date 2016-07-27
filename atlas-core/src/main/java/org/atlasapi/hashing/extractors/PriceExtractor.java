package org.atlasapi.hashing.extractors;

import com.metabroadcast.common.currency.Price;

public class PriceExtractor extends Extractor {

    private PriceExtractor() {
    }

    public static PriceExtractor create() {
        return new PriceExtractor();
    }

    @Override
    protected boolean isSupported(Object object) {
        return object instanceof Price;
    }

    @Override
    protected String extractValueInternal(Object object) {
        Price price = (Price) object;
        return price.getAmount() + price.getCurrency().getCurrencyCode();
    }
}
