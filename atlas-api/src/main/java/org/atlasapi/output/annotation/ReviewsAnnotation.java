package org.atlasapi.output.annotation;

import java.io.IOException;
import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.Content;
import org.atlasapi.entity.Review;
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
        writer.writeList(reviewsWriter, entity.getReviews(), ctxt);
    }
}
