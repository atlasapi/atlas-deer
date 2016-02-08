package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Alias;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class AliasWriter implements EntityListWriter<Alias> {

    @Override
    public void write(@Nonnull Alias entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("namespace", entity.getNamespace());
        writer.writeField("value", entity.getValue());
    }

    @Override
    public String fieldName(Alias entity) {
        return "alias";
    }

    @Override
    public String listName() {
        return "aliases";
    }

}
