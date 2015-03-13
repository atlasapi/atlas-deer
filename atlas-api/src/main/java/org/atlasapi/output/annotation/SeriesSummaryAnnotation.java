package org.atlasapi.output.annotation;

import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SeriesSummaryWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesSummaryAnnotation extends OutputAnnotation<Content> {

    public static final String SERIES_FIELD = "series";

    private final SeriesSummaryWriter summaryWriter;

    public SeriesSummaryAnnotation(SeriesSummaryWriter summaryWriter) {
        this.summaryWriter = checkNotNull(summaryWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Episode) {
            Episode episode = (Episode) entity;
            if(episode.getSeriesRef() == null) {
                writer.writeField(SERIES_FIELD, null);
            } else {
                summaryWriter.write(episode, writer, ctxt);
            }
        }
    }
}
