package org.atlasapi.output.annotation;

import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class AvailableContentAnnotation extends OutputAnnotation<Content> {

    private final ItemRefWriter itemRefWriter;

    public AvailableContentAnnotation(ItemRefWriter itemRefWriter) {
        this.itemRefWriter = checkNotNull(itemRefWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (!(entity instanceof Container)) {
            return;
        }

        Container container = (Container) entity;
        writer.writeList(
                itemRefWriter,
                container.getAvailableContent().keySet(),
                ctxt
        );

    }
}
