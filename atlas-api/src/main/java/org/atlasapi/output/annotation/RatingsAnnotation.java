package org.atlasapi.output.annotation;

import java.io.IOException;
import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.Content;
import org.atlasapi.entity.Rating;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class RatingsAnnotation extends OutputAnnotation<Content> {

    private EntityListWriter<Rating> ratingsWriter;

    public RatingsAnnotation(EntityListWriter<Rating> ratingsWriter) {
        super();
        this.ratingsWriter = checkNotNull(ratingsWriter);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(ratingsWriter, entity.getRatings(), ctxt);
    }
}
