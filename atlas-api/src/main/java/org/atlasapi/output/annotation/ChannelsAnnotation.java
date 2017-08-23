package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class ChannelsAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    public ChannelsAnnotation() {
        super();
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter format, OutputContext ctxt) throws IOException {
        // TODO Auto-generated method stub

    }

}
