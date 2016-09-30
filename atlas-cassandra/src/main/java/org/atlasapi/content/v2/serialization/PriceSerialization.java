package org.atlasapi.content.v2.serialization;

import java.util.Currency;

import org.atlasapi.content.v2.model.pojo.Price;

public class PriceSerialization {

    public Price serialize(com.metabroadcast.common.currency.Price price) {
        if (price == null) {
            return null;
        }
        Price internal =
                new Price();

        internal.setPrice(price.getAmount());
        Currency currency = price.getCurrency();
        if (currency != null) {
            internal.setCurrency(currency.getCurrencyCode());
        }

        return internal;
    }

    public com.metabroadcast.common.currency.Price deserialize(Price internalPrice) {
        return new com.metabroadcast.common.currency.Price(
                Currency.getInstance(internalPrice.getCurrency()),
                internalPrice.getPrice()
        );
    }
}
