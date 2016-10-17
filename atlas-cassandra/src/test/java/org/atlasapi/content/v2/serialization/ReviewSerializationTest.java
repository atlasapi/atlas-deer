package org.atlasapi.content.v2.serialization;

import java.util.Locale;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

import org.atlasapi.content.v2.model.udt.Review;


import org.junit.Test;

public class ReviewSerializationTest {

    private ReviewSerialization reviewSerialization;

    public ReviewSerializationTest() {
        reviewSerialization = new ReviewSerialization();
    }

    @Test
    public void serializeAndDeserializeReviews() {
        org.atlasapi.entity.Author author = org.atlasapi.entity.Author
                .builder()
                .withAuthorInitials("some initials")
                .withAuthorName("some name")
                .build();

        org.atlasapi.entity.Review review = new org.atlasapi.entity.Review(
                new Locale("usa"),
                "review",
                Optional.empty()
        );

        review.setType("type");
        review.setAuthor(author);

        Review serializedOutput = reviewSerialization.serialize(review);

        assertEquals(serializedOutput.getReview(), review.getReview());
        assertEquals(new Locale(serializedOutput.getLocale()), review.getLocale());
        assertEquals(serializedOutput.getType(), review.getType());
        assertEquals(
                serializedOutput.getAuthor().getAuthorInitials(),
                review.getAuthor().getAuthorInitials()
        );
        assertEquals(
                serializedOutput.getAuthor().getAuthorName(),
                review.getAuthor().getAuthorName()
        );

        org.atlasapi.entity.Review deserializedOutput =
                reviewSerialization.deserialize(serializedOutput);

        assertEquals(deserializedOutput.getReview(), review.getReview());
        assertEquals(deserializedOutput.getLocale(), review.getLocale());
        assertEquals(deserializedOutput.getType(), review.getType());
        assertEquals(
                deserializedOutput.getAuthor().getAuthorInitials(),
                review.getAuthor().getAuthorName()
        );
        assertEquals(
                deserializedOutput.getAuthor().getAuthorName(),
                review.getAuthor().getAuthorName()
        );
    }
}