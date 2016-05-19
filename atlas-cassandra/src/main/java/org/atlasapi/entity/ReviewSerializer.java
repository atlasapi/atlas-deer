package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;

import java.util.Locale;
import java.util.Optional;

public class ReviewSerializer {
    public CommonProtos.Review serialize(Review review) {
        CommonProtos.Review.Builder reviewBuilder = CommonProtos.Review.newBuilder();

        Locale locale = review.getLocale();
        if (null != locale) {
            CommonProtos.LocaleString.Builder localeBuilder = CommonProtos.LocaleString.newBuilder();
            localeBuilder.setValue(locale.getLanguage());
            reviewBuilder.setLocale(localeBuilder);
        }

        reviewBuilder.setReview(review.getReview());
        return reviewBuilder.build();
    }

    public Optional<Review> deserialize(CommonProtos.Review reviewBuffer) {
        Locale locale = null;
        if (reviewBuffer.hasLocale()) {
            locale = new Locale(reviewBuffer.getLocale().getValue());
        }

        // all the fields of this protocol buffer are optional for future
        // compatibility.  This incarnation requires .hasReview() to be true
        // to match Review requirements
        if (reviewBuffer.hasReview()) {
            return Optional.of(new Review(locale, reviewBuffer.getReview()));
        }

        return Optional.empty();
    }
}
