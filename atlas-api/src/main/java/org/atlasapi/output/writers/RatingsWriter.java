package org.atlasapi.output.writers;

import java.io.IOException;

import javax.annotation.Nonnull;
import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Rating;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class RatingsWriter implements EntityListWriter<Rating> {

    private final EntityWriter<Publisher> sourceWriter;

    public RatingsWriter(@Nonnull EntityWriter<Publisher> sourceWriter) {
        this.sourceWriter = checkNotNull(sourceWriter);
    }

    @Nonnull
    @Override
    public String listName() {
        return "ratings";
    }

    @Nonnull
    @Override
    public String fieldName(Rating entity) {
        return "rating";
    }

    @Override
    public void write(@Nonnull Rating entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt)
            throws IOException {

        writer.writeField("type", entity.getType());
        writer.writeField("value", entity.getValue());
        writer.writeField("numberOfVotes", entity.getNumberOfVotes());
        writer.writeObject(sourceWriter, entity.getPublisher(), ctxt);
    }

}
