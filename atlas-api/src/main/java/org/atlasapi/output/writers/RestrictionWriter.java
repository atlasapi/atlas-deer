package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.Restriction;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;


public class RestrictionWriter implements EntityListWriter<Restriction> {

    @Override
    public void write(Restriction entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("authority", entity.getAuthority());
        writer.writeField("rating", entity.getRating());
        writer.writeField("minimumAge", entity.getMinimumAge());
        writer.writeField("message", entity.getMessage());
    }

    @Override
    public String fieldName(Restriction entity) {
        return "restriction";
    }

    @Override
    public String listName() {
        return "restrictions";
    }

}
