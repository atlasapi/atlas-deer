package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.content.Film;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.v2.model.Content;

public class FilmSetter {

    private final ReleaseDateSerialization releaseDate = new ReleaseDateSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Film.class.isInstance(content)) {
            return;
        }

        Film film = (Film) content;

        internal.setWebsiteUrl(film.getWebsiteUrl());
        internal.setSubtitles(film.getSubtitles()
                .stream()
                .map(Subtitles::code)
                .collect(Collectors.toSet()));
        internal.setReleaseDates(film.getReleaseDates()
                .stream()
                .map(releaseDate::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Film film = (Film) content;

        film.setWebsiteUrl(internal.getWebsiteUrl());
        film.setSubtitles(internal.getSubtitles()
                .stream()
                .map(Subtitles::new)
                .collect(Collectors.toSet()));
        film.setReleaseDates(internal.getReleaseDates()
                .stream()
                .map(releaseDate::deserialize)
                .collect(Collectors.toSet()));
    }
}