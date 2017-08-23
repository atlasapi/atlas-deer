package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Identified;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class IdentificationAnnotation extends OutputAnnotation<Identified, ResolvedContent> {

    public IdentificationAnnotation() {
        super();
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter formatter, OutputContext ctxt)
            throws IOException {
        formatter.writeField("type", entity.getContent().getClass().getSimpleName().toLowerCase());
        //        if (entity != null && !(entity instanceof Channel)) {
        //            formatter.writeField("uri", entity.getCanonicalUri());
        //        }
    }

}
