package org.atlasapi.output.annotation;

import org.atlasapi.entity.Identified;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

public class CustomFieldsAnnotation extends OutputAnnotation<Identified> {

    @Override
    public void write(Identified entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeMap("custom_fields", entity.getCustomFields(), ctxt);
    }

}
