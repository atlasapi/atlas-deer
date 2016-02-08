package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.ReleaseDate;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public final class ReleaseDateWriter implements EntityListWriter<ReleaseDate> {

    @Override
    public void write(ReleaseDate entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("release_date", entity.date());
        writer.writeField("country", entity.country());
        writer.writeField("type", entity.type().toString().toLowerCase());
    }

    @Override
    public String listName() {
        return "release_dates";
    }

    @Override
    public String fieldName(ReleaseDate entity) {
        return "release_date";
    }
}