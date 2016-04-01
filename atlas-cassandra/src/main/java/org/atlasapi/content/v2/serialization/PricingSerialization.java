package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Pricing;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class PricingSerialization {

    private final PriceSerialization price = new PriceSerialization();

    public Pricing serialize(org.atlasapi.content.Pricing pricing) {
        if (pricing == null) {
            return null;
        }
        Pricing internal =
                new Pricing();

        internal.setStart(DateTimeUtils.toInstant(pricing.getStartTime()));
        internal.setEnd(DateTimeUtils.toInstant(pricing.getEndTime()));
        internal.setPrice(price.serialize(pricing.getPrice()));

        return internal;
    }

    public org.atlasapi.content.Pricing deserialize(Pricing internal) {
        return new org.atlasapi.content.Pricing(
                toDateTime(internal.getStart()),
                toDateTime(internal.getEnd()),
                price.deserialize(internal.getPrice())
        );
    }
}