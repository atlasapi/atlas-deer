package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.UsageType;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class UsageTypeWriter implements EntityWriter<UsageType> {

    @Override
    public void write(UsageType entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("name", entity.name());
        writer.writeField("title", entity.title());
    }

    @Override
    public String fieldName(UsageType entity) {
        return "usage_type";
    }

}
