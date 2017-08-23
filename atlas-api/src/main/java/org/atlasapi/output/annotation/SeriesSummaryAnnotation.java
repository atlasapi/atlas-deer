package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.SeriesSummaryWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesSummaryAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    public static final String SERIES_FIELD = "series";

    private final SeriesSummaryWriter seriesWriter;

    public SeriesSummaryAnnotation(SeriesSummaryWriter seriesSummaryWriter) {
        this.seriesWriter = checkNotNull(seriesSummaryWriter);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity.getContent() instanceof Episode) {
            Episode episode = (Episode) entity.getContent();
            if (episode.getSeriesRef() == null) {
                writer.writeField(SERIES_FIELD, null);
            } else {
                writer.writeObject(seriesWriter, episode, ctxt);
            }
        }
    }
}
