package org.atlasapi.output.annotation;

import java.io.IOException;

import com.google.common.base.MoreObjects;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public abstract class OutputAnnotation<T> {

    public abstract void write(T entity, FieldWriter writer, OutputContext ctxt) throws IOException;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
}
