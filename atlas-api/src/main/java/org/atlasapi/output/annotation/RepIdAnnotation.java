package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class RepIdAnnotation extends OutputAnnotation<Content> {

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        String repId = "patata";

        writer.writeField("rep_id", repId);
    }
}
