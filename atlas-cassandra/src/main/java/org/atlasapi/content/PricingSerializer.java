package org.atlasapi.content;

import com.metabroadcast.common.currency.Price;
import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.serialization.protobuf.ContentProtos;

import java.util.Currency;

public class PricingSerializer {

    public ContentProtos.Pricing serialize(Pricing pricing) {
        ContentProtos.Pricing.Builder builder = ContentProtos.Pricing.newBuilder();
        builder.setAmount(pricing.getPrice().getAmount());
        builder.setCurrency(pricing.getPrice().getCurrency().getCurrencyCode());
        if (pricing.getStartTime() != null) {
            builder.setStart(ProtoBufUtils.serializeDateTime(pricing.getStartTime()));
        }
        if (pricing.getEndTime() != null) {
            builder.setEnd(ProtoBufUtils.serializeDateTime(pricing.getEndTime()));
        }
        return builder.build();
    }

    public Pricing deserialize(ContentProtos.Pricing msg) {

        return new Pricing(
                msg.hasStart() ? ProtoBufUtils.deserializeDateTime(msg.getStart()) : null,
                msg.hasEnd() ? ProtoBufUtils.deserializeDateTime(msg.getEnd()) : null,
                new Price(Currency.getInstance(msg.getCurrency()), msg.getAmount())
        );
    }
}
