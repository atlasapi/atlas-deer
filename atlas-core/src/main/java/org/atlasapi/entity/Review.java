package org.atlasapi.entity;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;

public class Review implements Hashable {

    private final Locale locale;
    private final String review;
    private final String author;
    private final String authorInitials;
    private final String rating;
    private final DateTime date;
    private final ReviewType reviewType;
    // source is serialised as the publisher key
    private final Optional<Publisher> source;

    private Review(Builder builder) {
        this.review = checkNotNull(builder.review);
        this.source = checkNotNull(builder.source);

        this.reviewType = builder.reviewType;
        this.locale = builder.locale;
        this.author = builder.author;
        this.authorInitials = builder.authorInitials;
        this.rating = builder.rating;
        this.date = builder.date;
    }

    public static Builder builder(String review) {
        return new Builder(review);
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

    @Nullable
    public ReviewType getReviewType() {
        return reviewType;
    }

    @Nullable
    public String getAuthor() {
        return author;
    }

    @Nullable
    public String getAuthorInitials() {
        return authorInitials;
    }

    @Nullable
    public String getRating() {
        return rating;
    }

    @Nullable
    public DateTime getDate() {
        return date;
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
                Objects.equals(source, review1.source) &&
                Objects.equals(reviewType, review1.reviewType) &&
                Objects.equals(author, review1.author) &&
                Objects.equals(authorInitials, review1.authorInitials) &&
                Objects.equals(date, review1.date) &&
                Objects.equals(rating, review1.rating)
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale, review, source, reviewType, author, authorInitials, date, rating);
    }

    @Override
    public String toString() {
        return "Review{" +
                "locale=" + locale +
                ", review='" + review + '\'' +
                ", source='" + source + '\'' +
                ", reviewType='" + reviewType.toKey() + '\'' +
                ", author='" + author + '\'' +
                ", authorInitials='" + authorInitials + '\'' +
                ", date='" + date + '\'' +
                ", rating='" + rating + '\'' +
                '}';
    }

    public static final class Builder {

        private Locale locale;
        private String review;
        private String author;
        private String authorInitials;
        private String rating;
        private DateTime date;
        private ReviewType reviewType;
        private Optional<Publisher> source = Optional.empty();

        private Builder(String review) {
            this.review = review;
        }

        public Builder withLocale(@Nullable Locale locale) {
            this.locale = locale;
            return this;
        }

        public Builder withAuthor(@Nullable String author) {
            this.author = author;
            return this;
        }

        public Builder withAuthorInitials(@Nullable String authorInitials) {
            this.authorInitials = authorInitials;
            return this;
        }

        public Builder withRating(@Nullable String rating) {
            this.rating = rating;
            return this;
        }

        public Builder withDate(@Nullable DateTime date) {
            this.date = date;
            return this;
        }

        public Builder withReviewType(@Nullable ReviewType reviewType) {
            this.reviewType = reviewType;
            return this;
        }

        public Builder withSource(Optional<Publisher> source) {
            this.source = source;
            return this;
        }

        public Review build() {
            return new Review(this);
        }
    }
}
