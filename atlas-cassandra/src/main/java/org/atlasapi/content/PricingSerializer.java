package org.atlasapi.content;

import java.util.Currency;

import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.currency.Price;

public class PricingSerializer {

    public ContentProtos.Pricing serialize(Pricing pricing) {
        ContentProtos.Pricing.Builder builder = ContentProtos.Pricing.newBuilder();
        builder.setAmount(pricing.getPrice().getAmount());
        builder.setCurrency(pricing.getPrice().getCurrency().getCurrencyCode());
        if (pricing.getStartTime() != null) {
            builder.setStart(new DateTimeSerializer().serialize(pricing.getStartTime()));
        }
        if (pricing.getEndTime() != null) {
            builder.setEnd(new DateTimeSerializer().serialize(pricing.getEndTime()));
        }
        return builder.build();
    }

    public Pricing deserialize(ContentProtos.Pricing msg) {

        return new Pricing(
                msg.hasStart() ? new DateTimeSerializer().deserialize(msg.getStart()) : null,
                msg.hasEnd() ? new DateTimeSerializer().deserialize(msg.getEnd()) : null,
                new Price(Currency.getInstance(msg.getCurrency()), msg.getAmount())
        );
    }
}
