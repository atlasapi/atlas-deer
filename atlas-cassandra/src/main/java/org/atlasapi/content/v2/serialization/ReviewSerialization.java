package org.atlasapi.content.v2.serialization;

import java.util.Locale;
import java.util.Optional;

import org.atlasapi.content.v2.model.udt.Author;
import org.atlasapi.content.v2.model.udt.Review;

public class ReviewSerialization {

    public Review serialize(org.atlasapi.entity.Review review) {
        Review internal = new Review();
        internal.setReview(review.getReview());

        Locale locale = review.getLocale();
        if (locale != null) {
            internal.setLocale(locale.toLanguageTag());
        }

        internal.setType(review.getType());

        org.atlasapi.entity.Author author = review.getAuthor();
        if (author != null) {
            internal.setPeople(serializeAuthor(author));
        }

        return internal;
    }

    public org.atlasapi.entity.Review deserialize(Review review) {
        Locale locale = null;
        String internalLocale = review.getLocale();
        if (internalLocale != null) {
            locale = Locale.forLanguageTag(internalLocale);
        }

        org.atlasapi.entity.Review newReview = new org.atlasapi.entity.Review(
                locale, review.getReview(), Optional.empty());

        newReview.setType(review.getType());
        Author author = review.getAuthor();
        if (author != null) {
            newReview.setAuthor(deserializeAuthor(author));
        }

        return newReview;
    }

    private Author serializeAuthor(org.atlasapi.entity.Author authorOld) {
        Author newAuthor = new Author();
        newAuthor.setAuthorInitials(authorOld.getAuthorInitials());
        newAuthor.setAuthorName(authorOld.getAuthorName());

        return newAuthor;
    }

    private org.atlasapi.entity.Author deserializeAuthor(Author input) {
        return org.atlasapi.entity.Author
                .builder()
                .withAuthorInitials(input.getAuthorInitials())
                .withAuthorName(input.getAuthorName())
                .build();
    }
}
