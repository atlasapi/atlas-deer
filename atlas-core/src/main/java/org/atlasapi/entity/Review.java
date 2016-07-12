package org.atlasapi.entity;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class Review implements Hashable {

    private final Locale locale;
    private final String review;
    // source should not be serialised.  It is inherited from the Content that contains it
    private final Optional<Publisher> source;

    public Review(@Nullable Locale locale, String review, Optional<Publisher> source) {
        this.locale = locale;
        // note this is more strict than Owl (non-existent reviews should not be carried across)
        this.review = checkNotNull(review);

        // source of containing Content is stored for ease of rendering at API
        this.source = checkNotNull(source);
    }

    @Nullable
    public Locale getLocale() {
        return locale;
    }

    public String getReview() {
        return review;
    }

    public Optional<Publisher> getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Review review1 = (Review) o;
        return Objects.equals(locale, review1.locale) &&
                Objects.equals(review, review1.review) &&
                Objects.equals(source, review1.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale, review, source);
    }

    @Override
    public String toString() {
        return "Review{" +
                "locale=" + locale +
                ", review='" + review + '\'' +
                ", source=" + source +
                '}';
    }
}
