package org.atlasapi.content;


import javax.annotation.Nullable;
import java.util.Optional;

public class EpisodeSummary extends ItemSummary {

    private final Optional<Integer> episodeNumber;

    public EpisodeSummary(
            ItemRef itemRef,
            String title,
            @Nullable String description,
            @Nullable String image,
            @Nullable Integer episodeNumber) {
        super(itemRef, title, description, image);
        this.episodeNumber = Optional.ofNullable(episodeNumber);
    }

    public Optional<Integer> getEpisodeNumber() {
        return episodeNumber;
    }
}
