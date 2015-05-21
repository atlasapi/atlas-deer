package org.atlasapi.output.annotation;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BrandSeriesSummaryWriter;
import org.atlasapi.output.writers.EpisodesSeriesSummaryWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeriesSummaryAnnotation extends OutputAnnotation<Content> {

    public static final String SERIES_FIELD = "series";

    private final EpisodesSeriesSummaryWriter episodeWriter;
    private final BrandSeriesSummaryWriter brandWriter;

    public SeriesSummaryAnnotation(EpisodesSeriesSummaryWriter episodeWriter, BrandSeriesSummaryWriter brandWriter) {
        this.episodeWriter = checkNotNull(episodeWriter);
        this.brandWriter = checkNotNull(brandWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Episode) {
            Episode episode = (Episode) entity;
            if(episode.getSeriesRef() == null) {
                writer.writeField(SERIES_FIELD, null);
            } else {
                writer.writeObject(episodeWriter, episode, ctxt);
            }
        }
        if (entity instanceof Brand) {
            Brand brand = (Brand) entity;
            if (brand.getSeriesRefs() == null) {
                writer.writeField(SERIES_FIELD, null);
            } else {
                writer.writeObject(brandWriter, brand, ctxt);
            }
        }
        if (entity instanceof Brand) {
            Brand brand = (Brand) entity;
            if (brand.getSeriesRefs() == null) {
                writer.writeField();
            }
        }
    }
}
