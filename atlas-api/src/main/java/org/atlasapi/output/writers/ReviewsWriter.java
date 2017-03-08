package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Review;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ReviewsWriter implements EntityListWriter<Review> {

    private final EntityWriter<Publisher> sourceWriter;

    public ReviewsWriter(@Nonnull EntityWriter<Publisher> sourceWriter) {
        this.sourceWriter = checkNotNull(sourceWriter);
    }

    @Nonnull
    @Override
    public String listName() {
        return "reviews";
    }

    @Nonnull
    @Override
    public String fieldName(Review entity) {
        return "review";
    }

    @Override
    public void write(@Nonnull Review entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt)
            throws IOException {

        String language = (entity.getLocale() != null) ? entity.getLocale().getLanguage() : null;
        String reviewType = (entity.getReviewType() != null) ? entity.getReviewType().toKey() : null;

        writer.writeField("language", language);
        writer.writeField("review", entity.getReview());
        writer.writeField("author", entity.getAuthor());
        writer.writeField("author_initials", entity.getAuthorInitials());
        writer.writeField("rating", entity.getRating());
        writer.writeField("date", entity.getDate());
        writer.writeField("review_type", reviewType);

        Optional<Publisher> source = entity.getSource();
        if (source.isPresent()) {
            writer.writeObject(sourceWriter, source.get(), ctxt);
        }
    }
}
