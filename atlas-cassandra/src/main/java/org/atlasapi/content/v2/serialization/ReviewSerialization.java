package org.atlasapi.content.v2.serialization;

import java.util.Locale;
import java.util.Optional;

import com.google.common.base.Strings;
import org.atlasapi.content.v2.model.udt.Review;
import org.atlasapi.entity.ReviewType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.source.Sources;
import org.joda.time.Instant;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;
import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class ReviewSerialization {

    public Review serialize(org.atlasapi.entity.Review review) {
        Review internal = new Review();
        internal.setReview(review.getReview());

        Locale locale = review.getLocale();
        if (locale != null) {
            internal.setLocale(locale.toLanguageTag());
        }

        if (!Strings.isNullOrEmpty(review.getAuthor())) {
            internal.setAuthor(review.getAuthor());
        }

        if (!Strings.isNullOrEmpty(review.getAuthorInitials())) {
            internal.setAuthorInitials(review.getAuthorInitials());
        }

        if (!Strings.isNullOrEmpty(review.getRating())) {
            internal.setRating(review.getRating());
        }

        if (review.getDate() != null) {
            internal.setDate(toInstant(review.getDate()));
        }

        if (review.getReviewType() != null) {
            internal.setReviewTypeKey(review.getReviewType().toKey());
        }

        review.getSource().ifPresent(
                source -> internal.setPublisherKey(source.key())
        );

        return internal;
    }

    public org.atlasapi.entity.Review deserialize(Review review) {
        org.atlasapi.entity.Review.Builder reviewBuilder = org.atlasapi.entity.Review.builder(
                review.getReview()
        );

        Locale locale = null;
        String internalLocale = review.getLocale();
        if (internalLocale != null) {
            locale = Locale.forLanguageTag(internalLocale);
        }

        Instant instant = review.getDate();
        if (instant != null) {
            reviewBuilder.withDate(toDateTime(instant));
        }

        Publisher source = null;
        if (!Strings.isNullOrEmpty(review.getPublisherKey())) {
            source = Sources.fromPossibleKey(review.getPublisherKey()).orNull();
        }

        return reviewBuilder.withLocale(locale)
                .withAuthor(review.getAuthor())
                .withAuthorInitials(review.getAuthorInitials())
                .withRating(review.getRating())
                .withReviewType(ReviewType.fromKey(review.getReviewTypeKey()))
                .withSource(Optional.ofNullable(source))
                .build();
    }
}
