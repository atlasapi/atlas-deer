package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Episode;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.SeriesSummaryAnnotation;
import org.atlasapi.output.writers.common.CommonContainerSummaryWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesSummaryWriter implements EntityWriter<Episode> {

    private final NumberToShortStringCodec idCodec;
    private final ContainerSummaryResolver containerSummaryResolver;
    private final CommonContainerSummaryWriter commonContainerSummaryWriter;

    private SeriesSummaryWriter(
            NumberToShortStringCodec idCodec,
            ContainerSummaryResolver containerSummaryResolver,
            CommonContainerSummaryWriter commonContainerSummaryWriter
    ) {
        this.idCodec = checkNotNull(idCodec);
        this.containerSummaryResolver = checkNotNull(containerSummaryResolver);
        this.commonContainerSummaryWriter = checkNotNull(commonContainerSummaryWriter);
    }

    public static SeriesSummaryWriter create(
            NumberToShortStringCodec idCodec,
            ContainerSummaryResolver containerSummaryResolver,
            CommonContainerSummaryWriter commonContainerSummaryWriter
    ) {
        return new SeriesSummaryWriter(
                idCodec, containerSummaryResolver, commonContainerSummaryWriter
        );
    }

    @Override
    public void write(Episode entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        SeriesRef seriesRef = entity.getSeriesRef();
        if (seriesRef == null) {
            return;
        }
        writer.writeField("id", idCodec.encode(seriesRef.getId().toBigInteger()));

        Optional<ContainerSummary> summary = containerSummaryResolver.resolveContainerSummary(
                seriesRef.getId(),
                ctxt.getApplication(),
                ctxt.getActiveAnnotations()
        );

        if (summary.isPresent()) {
            commonContainerSummaryWriter.write(summary.get(), writer);
        }
    }

    @Override
    public String fieldName(Episode entity) {
        return SeriesSummaryAnnotation.SERIES_FIELD;
    }
}
