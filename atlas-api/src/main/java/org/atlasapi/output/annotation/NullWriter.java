package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class NullWriter<T> extends OutputAnnotation<T> {

    public static final <T> OutputAnnotation<T> create(Class<T> cls) {
        return new NullWriter<T>(cls);
    }

    public NullWriter(Class<T> cls) {
        super();
    }

    @Override
    public void write(Object entity, FieldWriter writer, OutputContext ctxt) throws IOException {

    }

}
