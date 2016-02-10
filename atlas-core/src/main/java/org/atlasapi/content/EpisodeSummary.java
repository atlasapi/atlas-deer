package org.atlasapi.content;

import java.util.Optional;

import javax.annotation.Nullable;

public class EpisodeSummary extends ItemSummary {

    private final Optional<Integer> episodeNumber;

    public EpisodeSummary(
            ItemRef itemRef,
            String title,
            @Nullable String description,
            @Nullable String image,
            @Nullable Integer episodeNumber,
            @Nullable Integer releaseYear,
            @Nullable Iterable<Certificate> certs) {
        super(itemRef, title, description, image, releaseYear, certs);
        this.episodeNumber = Optional.ofNullable(episodeNumber);
    }

    public Optional<Integer> getEpisodeNumber() {
        return episodeNumber;
    }
}
