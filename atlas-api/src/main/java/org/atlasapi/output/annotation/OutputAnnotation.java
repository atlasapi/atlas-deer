package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.base.Objects;

public abstract class OutputAnnotation<T, R> {

    public abstract void write(R entity, FieldWriter writer, OutputContext ctxt) throws IOException;

    @Override
    public String toString() {
        return Objects.toStringHelper(this).toString();
    }
}
