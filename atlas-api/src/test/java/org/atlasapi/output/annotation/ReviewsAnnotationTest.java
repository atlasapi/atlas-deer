package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.content.Content;
import org.atlasapi.content.Film;
import org.atlasapi.entity.Review;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.writers.ReviewsWriter;
import org.atlasapi.output.writers.SourceWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReviewsAnnotationTest {

    private Content content;
    private ReviewsAnnotation reviewsAnnotation;

    @Before
    public void setUp() {
        content = mock(Film.class);

        EntityWriter<Publisher> publisherWriter = SourceWriter.sourceWriter("source");
        EntityListWriter<Review> reviewWriter = new ReviewsWriter(publisherWriter);
        reviewsAnnotation = new ReviewsAnnotation(reviewWriter);

        when(content.getReviews()).thenReturn(populateReviews());
        when(content.getSource()).thenReturn(Publisher.RADIO_TIMES);

    }

    @Test
    public void missingSourcesAreAddedToReviewsFromContent() throws Exception {
        Iterable<Review> actualReviews = reviewsAnnotation.populateMissingReviewSources(
                content.getReviews(),
                content.getSource()
        );

        for (Review review : actualReviews) {
            assertTrue(review.getSource().isPresent());

            if (review.getReview().equals("No publisher")) {
                assertThat(review.getSource().get(), is(content.getSource()));
            } else {
                assertThat(review.getSource().get(), is(Publisher.BBC));
            }
        }
    }

    private Set<Review> populateReviews() {
        return ImmutableSet.of(
                Review.builder("No publisher").build(),
                Review.builder("Has publisher")
                        .withSource(Optional.of(Publisher.BBC))
                        .build(),
                Review.builder("No publisher").build(),
                Review.builder("Has publisher")
                        .withSource(Optional.of(Publisher.BBC))
                        .build()
        );
    }
}
