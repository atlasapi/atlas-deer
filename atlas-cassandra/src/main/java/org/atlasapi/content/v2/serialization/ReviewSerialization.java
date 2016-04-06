package org.atlasapi.content.v2.serialization;

import java.util.Locale;
import java.util.Optional;

import org.atlasapi.content.v2.model.udt.Review;

public class ReviewSerialization {

    public Review serialize(org.atlasapi.entity.Review review) {
        Review internal = new Review();
        internal.setReview(review.getReview());

        Locale locale = review.getLocale();
        if (locale != null) {
            internal.setLocale(locale.toLanguageTag());
        }

        return internal;
    }

    public org.atlasapi.entity.Review deserialize(Review review) {
        Locale locale = null;
        String internalLocale = review.getLocale();
        if (internalLocale != null) {
            locale = Locale.forLanguageTag(internalLocale);
        }

        return new org.atlasapi.entity.Review(locale, review.getReview(), Optional.empty());
    }
}
