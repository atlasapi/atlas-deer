package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Item;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.common.CommonContainerSummaryWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerSummaryWriter implements EntityWriter<Item> {

    private final String containerField;
    private final NumberToShortStringCodec idCodec;
    private final ContainerSummaryResolver containerSummaryResolver;
    private final CommonContainerSummaryWriter commonContainerSummaryWriter;

    private ContainerSummaryWriter(
            NumberToShortStringCodec idCodec,
            String containerField,
            ContainerSummaryResolver containerSummaryResolver,
            CommonContainerSummaryWriter commonContainerSummaryWriter
    ) {
        this.containerField = checkNotNull(containerField);
        this.idCodec = checkNotNull(idCodec);
        this.containerSummaryResolver = checkNotNull(containerSummaryResolver);
        this.commonContainerSummaryWriter = checkNotNull(commonContainerSummaryWriter);
    }

    public static ContainerSummaryWriter create(
            NumberToShortStringCodec idCodec,
            String containerField,
            ContainerSummaryResolver containerSummaryResolver,
            CommonContainerSummaryWriter commonContainerSummaryWriter
    ) {
        return new ContainerSummaryWriter(
                idCodec, containerField, containerSummaryResolver, commonContainerSummaryWriter
        );
    }

    @Override
    public void write(Item entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ContainerRef container = entity.getContainerRef();
        writer.writeField("id", idCodec.encode(container.getId().toBigInteger()));

        Optional<ContainerSummary> summary = Optional.fromNullable(entity.getContainerSummary());
        if (!summary.isPresent()) {
            summary = containerSummaryResolver.resolveContainerSummary(
                    container.getId(),
                    ctxt.getApplicationSources(),
                    ctxt.getActiveAnnotations()
            );
        }
        if (summary.isPresent()) {
            commonContainerSummaryWriter.write(summary.get(), writer);
        }
    }

    @Override
    public String fieldName(Item entity) {
        return containerField;
    }
}
