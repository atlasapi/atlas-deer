package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class LicenseWriter extends OutputAnnotation<Object, Object> {

    public LicenseWriter() {
        super();
    }

    @Override
    public void write(Object entity, FieldWriter writer, OutputContext ctxt) throws IOException {
    }

}
