package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ContainerSummaryWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerSummaryAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final String containerField;
    private final ContainerSummaryWriter summaryWriter;

    public ContainerSummaryAnnotation(String containerField, ContainerSummaryWriter summaryWriter) {
        this.containerField = checkNotNull(containerField);
        this.summaryWriter = checkNotNull(summaryWriter);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity.getContent() instanceof Item) {
            Item item = (Item) entity.getContent();
            if (item.getContainerRef() == null) {
                writer.writeField(containerField, null);
            } else {
                writer.writeObject(summaryWriter, item, ctxt);
            }
        }
    }

}
