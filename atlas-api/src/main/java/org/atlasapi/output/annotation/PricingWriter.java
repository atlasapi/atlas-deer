package org.atlasapi.output.annotation;

import org.atlasapi.content.Pricing;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

public class PricingWriter implements EntityListWriter<Pricing> {
    @Override
    public String listName() {
        return "pricing";
    }

    @Override
    public void write(Pricing entity, FieldWriter writer, OutputContext ctxt) throws IOException {

        writer.writeField("currency", entity.getPrice().getCurrency());
        writer.writeField("price", entity.getPrice().getAmount());
        writer.writeField("start_time", entity.getStartTime());
        writer.writeField("end_time", entity.getEndTime());

    }

    @Override
    public String fieldName(Pricing entity) {
        return "pricing";
    }
}
