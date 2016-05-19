package org.atlasapi.entity;

import java.util.Objects;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;
import java.util.Locale;

public class Review {

    private final Locale locale;
    private final String review;

    public Review(@Nullable Locale locale, String review) {
        this.locale = locale;
        // note this is more strict than Owl (non-existent reviews should not be carried across)
        this.review = checkNotNull(review);
    }

    @Nullable
    public Locale getLocale() {
        return locale;
    }

    public String getReview() {
        return review;
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
                Objects.equals(review, review1.review);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale, review);
    }

    @Override
    public String toString() {
        String safeLocale = (null != locale) ? locale.toString() : "";

        return "Review{" +
                "locale=" + safeLocale +
                ", review='" + review + '\'' +
                '}';
    }
}
