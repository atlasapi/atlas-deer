package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class NullWriter<T, R> extends OutputAnnotation<T, R> {

    public static final <T, R> OutputAnnotation<T, R> create(Class<R> cls) {
        return new NullWriter<T, R>(cls);
    }

    public NullWriter(Class<R> cls) {
        super();
    }

    @Override
    public void write(Object entity, FieldWriter writer, OutputContext ctxt) throws IOException {

    }

}
