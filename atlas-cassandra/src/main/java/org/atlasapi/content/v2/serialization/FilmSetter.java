package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Film;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.ReleaseDate;

public class FilmSetter {

    private final ReleaseDateSerialization releaseDate = new ReleaseDateSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Film.class.isInstance(content)) {
            return;
        }

        Film film = (Film) content;

        internal.setWebsiteUrl(film.getWebsiteUrl());

        Set<Subtitles> subtitles = film.getSubtitles();
        if (subtitles != null) {
            internal.setSubtitles(subtitles
                    .stream()
                    .map(Subtitles::code)
                    .collect(Collectors.toSet()));
        }

        Set<org.atlasapi.content.ReleaseDate> releaseDates = film.getReleaseDates();
        if (releaseDates != null) {
            internal.setReleaseDates(releaseDates
                    .stream()
                    .map(releaseDate::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Film film = (Film) content;

        film.setWebsiteUrl(internal.getWebsiteUrl());

        Set<String> subtitles = internal.getSubtitles();
        if (subtitles != null) {
            film.setSubtitles(subtitles
                    .stream()
                    .map(Subtitles::new)
                    .collect(Collectors.toSet()));
        }

        Set<ReleaseDate> releaseDates = internal.getReleaseDates();
        if (releaseDates != null) {
            film.setReleaseDates(releaseDates
                    .stream()
                    .map(releaseDate::deserialize)
                    .collect(Collectors.toSet()));
        }
    }
}