package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

import javax.annotation.Nullable;

@UDT(name = "review")
public class Review {

    @Field(name = "locale") private String locale;
    @Field(name = "review") private String review;
    @Field(name = "author") private String author;
    @Field(name = "authorInitials") private String authorInitials;
    @Field(name = "rating") private String rating;
    @Field(name = "date") private Instant date;
    @Field(name = "reviewTypeKey") private String reviewTypeKey;

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

    public void setDate(Instant date) {
        this.date = date;
    }

    @Nullable
    public String getReviewTypeKey() {
        return reviewTypeKey;
    }

    public void setReviewTypeKey(String reviewTypeKey) {
        this.reviewTypeKey = reviewTypeKey;
    }
}
