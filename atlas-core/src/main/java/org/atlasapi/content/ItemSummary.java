package org.atlasapi.content;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import org.atlasapi.entity.Sameable;
import org.atlasapi.hashing.Hashable;

import javax.annotation.Nullable;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemSummary implements Hashable, Sameable {

    public static final Ordering<ItemSummary> ORDERING = new Ordering<ItemSummary>() {

        @Override
        public int compare(@Nullable ItemSummary left, @Nullable ItemSummary right) {
            if (left instanceof EpisodeSummary && right instanceof EpisodeSummary) {
                EpisodeSummary leftES = (EpisodeSummary) left;
                EpisodeSummary rightES = (EpisodeSummary) right;
                return leftES.getEpisodeNumber().orElse(Integer.MAX_VALUE).compareTo(
                        rightES.getEpisodeNumber().orElse(Integer.MAX_VALUE)
                );
            } else if (left instanceof EpisodeSummary && ((EpisodeSummary) left).getEpisodeNumber()
                    .isPresent()) {
                return -1;
            } else if (right instanceof EpisodeSummary
                    && ((EpisodeSummary) right).getEpisodeNumber().isPresent()) {
                return 1;
            }
            return 0;
        }
    };

    private final ItemRef itemRef;
    private final String title;
    private final Optional<String> description;
    private final Optional<String> image;
    private final Optional<Integer> releaseYear;
    private final Optional<ImmutableSet<Certificate>> certificate;

    public ItemSummary(ItemRef itemRef, String title, @Nullable String description,
            @Nullable String image, @Nullable Integer releaseYear,
            @Nullable Iterable<Certificate> certs) {
        this.itemRef = checkNotNull(itemRef);
        this.title = title;
        this.description = Optional.ofNullable(description);
        this.image = Optional.ofNullable(image);
        this.releaseYear = Optional.ofNullable(releaseYear);
        this.certificate = Optional.ofNullable(certs != null
                                               ? ImmutableSet.copyOf(certs)
                                               : ImmutableSet.of());

    }

    public ItemRef getItemRef() {
        return itemRef;
    }

    public String getTitle() {
        return title;
    }

    public Optional<String> getImage() {
        return image;
    }

    public Optional<Integer> getReleaseYear() {
        return releaseYear;
    }

    public Optional<ImmutableSet<Certificate>> getCertificates() {
        return certificate;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(itemRef);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ItemSummary)) {
            return false;
        }
        ItemSummary that = (ItemSummary) o;
        return Objects.equal(itemRef, that.itemRef);
    }

    public Optional<String> getDescription() {
        return description;
    }

    @Override
    public boolean isSame(@Nullable Sameable other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        ItemSummary that = (ItemSummary) other;
        return itemRef.isSame(that.itemRef) &&
                java.util.Objects.equals(title, that.title) &&
                java.util.Objects.equals(description, that.description) &&
                java.util.Objects.equals(image, that.image) &&
                java.util.Objects.equals(releaseYear, that.releaseYear) &&
                java.util.Objects.equals(certificate, that.certificate);
    }
}
