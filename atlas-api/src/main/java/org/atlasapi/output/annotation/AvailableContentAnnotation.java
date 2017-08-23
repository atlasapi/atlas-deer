package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class AvailableContentAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final ItemRefWriter itemRefWriter;

    public AvailableContentAnnotation(ItemRefWriter itemRefWriter) {
        this.itemRefWriter = checkNotNull(itemRefWriter);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (!(entity.getContent() instanceof Container)) {
            return;
        }

        Container container = (Container) entity.getContent();
        writer.writeList(
                itemRefWriter,
                container.getAvailableContent().keySet(),
                ctxt
        );

    }
}
