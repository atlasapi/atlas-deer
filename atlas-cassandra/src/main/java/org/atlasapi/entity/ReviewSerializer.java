package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;

import java.util.Locale;

public class ReviewSerializer {
    public CommonProtos.Review serialize(Review review) {
        CommonProtos.Review.Builder pbb = CommonProtos.Review.newBuilder();

        Locale locale = review.getLocale();
        if (null != locale) {
            CommonProtos.LocaleString.Builder lppb = CommonProtos.LocaleString.newBuilder();
            lppb.setValue(locale.getLanguage());
            pbb.setLocale(lppb);
        }

        pbb.setReview(review.getReview());
        return pbb.build();
    }

    public Review deserialize(CommonProtos.Review reviewPb) {
        Locale locale = null;
        if (reviewPb.hasLocale()) {
            locale = new Locale(reviewPb.getLocale().getValue());
        }

        return new Review(locale, reviewPb.getReview());
    }
}
