package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class IsPublishedAnnotation extends OutputAnnotation<Content> {

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("is_published", entity.isActivelyPublished());
    }
}
