package org.atlasapi.query.v4.event;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SourceWriter;

import java.io.IOException;

public class ContentGroupListWriter<V extends ContentGroup> implements EntityListWriter<V> {
    private final EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");

    @Override
    public String listName() {
        return "content_groups";
    }

    @Override
    public void write(V entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("title", entity.getTitle());
        writer.writeObject(publisherWriter, entity.getSource(), ctxt);
        writer.writeField("type", entity.getType().name());
    }

    @Override
    public String fieldName(V entity) {
        return "content_group";
    }
}
