package org.atlasapi.output.annotation;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ContainerSummaryWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerSummaryAnnotation extends OutputAnnotation<Content> {

    private final String containerField;
    private final ContainerSummaryWriter summaryWriter;
    private final ResourceRefWriter brandRefWriter;

    public ContainerSummaryAnnotation(
            String containerField,
            ContainerSummaryWriter summaryWriter,
            ResourceRefWriter brandRefWriter
    ) {
        this.containerField = checkNotNull(containerField);
        this.summaryWriter = checkNotNull(summaryWriter);
        this.brandRefWriter = checkNotNull(brandRefWriter);
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
        } else if (entity instanceof Series) {
            Series series = (Series) entity;
            if (series.getBrandRef() == null) {
                writer.writeField(containerField, null);
            } else {
                writer.writeObject(brandRefWriter, series.getBrandRef(), ctxt);
            }
        }
    }

}
