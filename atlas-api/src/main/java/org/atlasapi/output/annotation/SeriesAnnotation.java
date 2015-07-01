package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SeriesWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesAnnotation extends OutputAnnotation<Content> {

    private final SeriesWriter seriesWriter;

    public SeriesAnnotation(SeriesWriter seriesWriter) {
        this.seriesWriter = checkNotNull(seriesWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity instanceof Brand)) {
            return;
        }
        Brand brand = (Brand) entity;
        if (brand.getSeriesRefs() == null || brand.getSeriesRefs().isEmpty()) {
            return;
        }
        writer.writeList(seriesWriter, brand.getSeriesRefs(), ctxt);
    }
}
