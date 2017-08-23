package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SeriesWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final SeriesWriter seriesWriter;

    public SeriesAnnotation(SeriesWriter seriesWriter) {
        this.seriesWriter = checkNotNull(seriesWriter);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity.getContent() instanceof Brand)) {
            return;
        }
        Brand brand = (Brand) entity.getContent();
        if (brand.getSeriesRefs() == null || brand.getSeriesRefs().isEmpty()) {
            return;
        }
        writer.writeList(seriesWriter, brand.getSeriesRefs(), ctxt);
    }
}
