package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class PublisherAnnotation extends OutputAnnotation<Content, Content> {

    public PublisherAnnotation() {
        super();
    }

    @Override
    public void write(Content entity, FieldWriter format, OutputContext ctxt) throws IOException {
        // TODO Auto-generated method stub

    }

}
