package org.atlasapi.output.writers;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Item;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerSummaryWriter implements EntityWriter<Item> {

    private final String containerField;
    private final NumberToShortStringCodec idCodec;
    private final ContainerSummaryResolver containerSummaryResolver;

    public ContainerSummaryWriter(NumberToShortStringCodec idCodec, String containerField, ContainerSummaryResolver containerSummaryResolver) {
        this.containerField = checkNotNull(containerField);
        this.idCodec = checkNotNull(idCodec);
        this.containerSummaryResolver = checkNotNull(containerSummaryResolver);
    }

    @Override
    public void write(Item entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ContainerRef container = entity.getContainerRef();
        writer.writeField("id", idCodec.encode(container.getId().toBigInteger()));

        Optional<ContainerSummary> summary = Optional.fromNullable(entity.getContainerSummary());
        if(!summary.isPresent()) {
            summary = containerSummaryResolver.resolveContainerSummary(container.getId(), ctxt.getApplicationSources(), ctxt.getActiveAnnotations());
        }
        if (summary.isPresent()) {
            writer.writeField("type", summary.get().getType().toLowerCase());
            writer.writeField("title", summary.get().getTitle());
            writer.writeField("description", summary.get().getDescription());
            if (summary.get().getSeriesNumber() != null) {
                writer.writeField("series_number", summary.get().getSeriesNumber());
            }
        }
    }

    @Override
    public String fieldName(Item entity) {
        return containerField;
    }
}
