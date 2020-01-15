package org.atlasapi.output.annotation;

import org.atlasapi.content.Content;
import org.atlasapi.entity.Award;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;

public class AwardsAnnotation extends OutputAnnotation<Content> {

    public AwardsAnnotation() {
        super();
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(new EntityListWriter<Award>() {

            @Override
            public void write(@Nonnull Award entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
                writer.writeField("title", entity.getTitle());
                writer.writeField("description", entity.getDescription());
                writer.writeField("year", entity.getYear());
                writer.writeField("outcome", entity.getOutcome());
            }

            @Nonnull
            @Override
            public String fieldName(Award entity) {
                return "award";
            }

            @Nonnull
            @Override
            public String listName() {
                return "awards";
            }
        }, entity.getAwards(), ctxt);
    }

}
