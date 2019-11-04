package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.Objects;

@UDT(name = "review")
public class Review {

    @Field(name = "locale") private String locale;
    @Field(name = "review") private String review;
    @Field(name = "author") private String author;
    @Field(name = "author_initials") private String authorInitials;
    @Field(name = "rating") private String rating;
    @Field(name = "date") private Instant date;
    @Field(name = "review_type_key") private String reviewTypeKey;
    @Field(name = "publisher_key") private String publisherKey;

    public Review() {}

    @Nullable
    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    @Nullable
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @Nullable
    public String getAuthorInitials() {
        return authorInitials;
    }

    public void setAuthorInitials(String authorInitials) {
        this.authorInitials = authorInitials;
    }

    @Nullable
    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }

    @Nullable
    public Instant getDate() {
        return date;
    }

    public void setDate(@Nullable Instant date) {
        this.date = date;
    }

    @Nullable
    public String getReviewTypeKey() {
        return reviewTypeKey;
    }

    public void setReviewTypeKey(String reviewTypeKey) {
        this.reviewTypeKey = reviewTypeKey;
    }

    @Nullable
    public String getPublisherKey() {
        return publisherKey;
    }

    public void setPublisherKey(String publisherKey) {
        this.publisherKey = publisherKey;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Review review1 = (Review) object;
        return Objects.equals(locale, review1.locale) &&
                Objects.equals(review, review1.review) &&
                Objects.equals(author, review1.author) &&
                Objects.equals(authorInitials, review1.authorInitials) &&
                Objects.equals(rating, review1.rating) &&
                Objects.equals(date, review1.date) &&
                Objects.equals(reviewTypeKey, review1.reviewTypeKey) &&
                Objects.equals(publisherKey, review1.publisherKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale, review, author, authorInitials, rating, date, reviewTypeKey, publisherKey);
    }
}
