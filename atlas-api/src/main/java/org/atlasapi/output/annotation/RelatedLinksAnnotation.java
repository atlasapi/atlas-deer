package org.atlasapi.output.annotation;


import java.io.IOException;

import org.atlasapi.media.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.RelatedLinkWriter;


public class RelatedLinksAnnotation extends OutputAnnotation<Content> {

    public RelatedLinksAnnotation() {
        super();
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(new RelatedLinkWriter(), entity.getRelatedLinks(), ctxt);
    }

}
