package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class SeriesReferenceAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private static final String SERIES_FIELD = "series";

    private final ResourceRefWriter seriesRefWriter;

    public SeriesReferenceAnnotation(NumberToShortStringCodec idCodec) {
        seriesRefWriter = new ResourceRefWriter(SERIES_FIELD, idCodec);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity.getContent() instanceof Episode) {
            Episode episode = (Episode) entity.getContent();
            if (episode.getSeriesRef() == null) {
                writer.writeField(SERIES_FIELD, null);
            } else {
                writer.writeObject(seriesRefWriter, episode.getSeriesRef(), ctxt);
            }
        }
    }
}
