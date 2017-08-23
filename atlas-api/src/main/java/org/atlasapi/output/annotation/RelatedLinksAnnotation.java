package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.RelatedLinkWriter;

public class RelatedLinksAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    public RelatedLinksAnnotation() {
        super();
    }

    @Override
    public void write(ResolvedContent
            entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(new RelatedLinkWriter(), entity.getContent().getRelatedLinks(), ctxt);
    }

}
