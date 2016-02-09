package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SourceWriter implements EntityListWriter<Publisher> {

    public static final EntityListWriter<Publisher> sourceListWriter(String listName) {
        return new SourceWriter(checkNotNull(listName), "source");
    }

    public static final EntityWriter<Publisher> sourceWriter(String fieldName) {
        return new SourceWriter(null, checkNotNull(fieldName));
    }

    private final String listName;
    private final String fieldName;

    private SourceWriter(String listName, String fieldName) {
        this.listName = listName;
        this.fieldName = fieldName;
    }

    @Override
    public void write(Publisher entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("key", entity.key());
        writer.writeField("name", entity.title());
        writer.writeField("country", entity.country());
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(Publisher entity) {
        return fieldName;
    }
}