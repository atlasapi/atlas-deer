package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Review;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;


public class ReviewsAnnotation extends OutputAnnotation<Content> {

    private EntityListWriter<Review> reviewsWriter;

    public ReviewsAnnotation(EntityListWriter<Review> reviewsWriter) {
        super();
        this.reviewsWriter = checkNotNull(reviewsWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(
                reviewsWriter,
                populateMissingReviewSources(entity.getReviews(), entity.getSource()),
                ctxt
        );
    }

    Iterable<Review> populateMissingReviewSources(Set<Review> reviews, Publisher publisher) {

        return reviews.stream()
                .map(review -> {
                    if (!review.getSource().isPresent()) {
                        return Review.builder(review.getReview())
                                .withLocale(review.getLocale())
                                .withDate(review.getDate())
                                .withAuthorInitials(review.getAuthorInitials())
                                .withAuthor(review.getAuthor())
                                .withRating(review.getRating())
                                .withReviewType(review.getReviewType())
                                .withSource(Optional.of(publisher))
                                .build();
                    }
                    return review;
                })
                .collect(MoreCollectors.toImmutableSet());
    }
}
