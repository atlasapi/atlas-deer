package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ContainerSummaryWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerSummaryAnnotation extends OutputAnnotation<Content> {

    private final String containerField;
    private final ContainerSummaryWriter summaryWriter;

    public ContainerSummaryAnnotation(String containerField, ContainerSummaryWriter summaryWriter) {
        this.containerField = checkNotNull(containerField);
        this.summaryWriter = checkNotNull(summaryWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            if (item.getContainerRef() == null) {
                writer.writeField(containerField, null);
            } else {
                writer.writeObject(summaryWriter, item, ctxt);
            }
        }
    }

}
