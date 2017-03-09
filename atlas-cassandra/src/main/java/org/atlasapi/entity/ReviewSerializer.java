package org.atlasapi.entity;

import com.google.common.base.Strings;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.source.Sources;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.Locale;
import java.util.Optional;

public class ReviewSerializer {

    private final DateTimeSerializer dateTimeSerializer;

    private ReviewSerializer() {
        this.dateTimeSerializer = new DateTimeSerializer();
    }

    public static ReviewSerializer create() {
        return new ReviewSerializer();
    }

    public CommonProtos.Review serialize(Review review) {
        CommonProtos.Review.Builder reviewBuilder = CommonProtos.Review.newBuilder();

        Locale locale = review.getLocale();
        if (locale != null) {
            CommonProtos.LocaleString.Builder localeBuilder = CommonProtos.LocaleString.newBuilder();
            localeBuilder.setValue(locale.getLanguage());
            reviewBuilder.setLocale(localeBuilder);
        }

        if (review.getDate() != null) {
            reviewBuilder.setDate(dateTimeSerializer.serialize(review.getDate()));
        }

        if (!Strings.isNullOrEmpty(review.getAuthor())) {
            reviewBuilder.setAuthor(review.getAuthor());
        }

        if (!Strings.isNullOrEmpty(review.getAuthorInitials())) {
            reviewBuilder.setAuthorInitials(review.getAuthorInitials());
        }

        if (!Strings.isNullOrEmpty(review.getRating())) {
            reviewBuilder.setRating(review.getRating());
        }

        if (review.getReviewType() != null) {
            reviewBuilder.setReviewType(review.getReviewType().toKey());
        }

        review.getSource().ifPresent(
                source -> reviewBuilder.setPublisherKey(source.key())
        );
        reviewBuilder.setReview(review.getReview());

        return reviewBuilder.build();
    }

    public Optional<Review> deserialize(CommonProtos.Review reviewBuffer) {
        // * all the fields of this protocol buffer are optional for future
        //   compatibility.  This incarnation requires .hasReview() to be true
        //    to match Review requirements
        // * source is inherited from the Item containing the review

        if (!reviewBuffer.hasReview()) {
            return Optional.empty();
        }

        Review.Builder reviewBuilder = Review.builder(reviewBuffer.getReview());

        if (reviewBuffer.hasLocale()) {
            reviewBuilder.withLocale(new Locale(reviewBuffer.getLocale().getValue()));
        }

        if (reviewBuffer.hasDate()) {
            reviewBuilder.withDate(dateTimeSerializer.deserialize(reviewBuffer.getDate()));
        }

        if (reviewBuffer.hasReviewType()) {
            reviewBuilder.withReviewType(ReviewType.fromKey(reviewBuffer.getReviewType()));
        }

        if (reviewBuffer.hasAuthor()) {
            reviewBuilder.withAuthor(reviewBuffer.getAuthor());
        }

        if (reviewBuffer.hasAuthorInitials()) {
            reviewBuilder.withAuthorInitials(reviewBuffer.getAuthorInitials());
        }

        if (reviewBuffer.hasRating()) {
            reviewBuilder.withRating(reviewBuffer.getRating());
        }

        if (reviewBuffer.hasPublisherKey()) {
            Publisher publisher = Sources.fromPossibleKey(reviewBuffer.getPublisherKey()).orNull();
            reviewBuilder.withSource(Optional.ofNullable(publisher));
        }

        return Optional.of(reviewBuilder.build());

    }
}
