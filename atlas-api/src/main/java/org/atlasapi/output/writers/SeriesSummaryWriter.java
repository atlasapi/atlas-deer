package org.atlasapi.output.writers;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.Episode;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.SeriesSummaryAnnotation;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesSummaryWriter implements EntityWriter<Episode> {

    private final NumberToShortStringCodec idCodec;
    private final ContainerSummaryResolver containerSummaryResolver;

    public SeriesSummaryWriter(NumberToShortStringCodec idCodec, ContainerSummaryResolver containerSummaryResolver) {
        this.idCodec = checkNotNull(idCodec);
        this.containerSummaryResolver = checkNotNull(containerSummaryResolver);
    }

    @Override
    public void write(Episode entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        SeriesRef seriesRef = entity.getSeriesRef();
        if(seriesRef == null) {
            return;
        }
        writer.writeField("id", idCodec.encode(seriesRef.getId().toBigInteger()));

        Optional<ContainerSummary> summary = containerSummaryResolver.resolveContainerSummary(
                seriesRef.getId(),
                ctxt.getApplicationSources()
        );
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
    public String fieldName(Episode entity) {
        return SeriesSummaryAnnotation.SERIES_FIELD;
    }
}
