package org.atlasapi.output.writers;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.content.Brand;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.ContainerSummaryResolver;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.SeriesSummaryAnnotation;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class BrandSeriesSummaryWriter implements EntityWriter<Brand> {

    private final ContainerSummaryResolver resolver;
    private final NumberToShortStringCodec idCodec;

    public BrandSeriesSummaryWriter(NumberToShortStringCodec codec, ContainerSummaryResolver resolver) {
        this.resolver = checkNotNull(resolver);
        this.idCodec = checkNotNull(codec);
    }

    @Override
    public void write(@Nonnull Brand entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        if (entity.getSeriesRefs() == null || entity.getSeriesRefs().isEmpty()) {
            return;
        }
        for (SeriesRef ref : entity.getSeriesRefs()) {
            writer.writeField("id", idCodec.encode(ref.getId().toBigInteger()));

            Optional<ContainerSummary> summary = resolver.resolveContainerSummary(
                    ref.getId(),
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
    }

    @Nonnull
    @Override
    public String fieldName(Brand entity) {
        return SeriesSummaryAnnotation.SERIES_FIELD;
    }
}
