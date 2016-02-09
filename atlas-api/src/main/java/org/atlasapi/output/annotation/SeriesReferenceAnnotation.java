package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class SeriesReferenceAnnotation extends OutputAnnotation<Content> {

    private static final String SERIES_FIELD = "series";

    private final ResourceRefWriter seriesRefWriter;

    public SeriesReferenceAnnotation(NumberToShortStringCodec idCodec) {
        seriesRefWriter = new ResourceRefWriter(SERIES_FIELD, idCodec);
    }

    @Override
    public void write(Content content, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (content instanceof Episode) {
            Episode episode = (Episode) content;
            if (episode.getSeriesRef() == null) {
                writer.writeField(SERIES_FIELD, null);
            } else {
                writer.writeObject(seriesRefWriter, episode.getSeriesRef(), ctxt);
            }
        }
    }
}
