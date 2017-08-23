package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.FieldInfoWriter;
import org.atlasapi.query.v4.meta.LinkCreator;

public class ModelInfoAnnotation<T extends ModelClassInfo> extends OutputAnnotation<T, T> {

    private final EntityListWriter<? super FieldInfo> fieldInfoWriter;

    public ModelInfoAnnotation(LinkCreator linkCreator) {
        this.fieldInfoWriter = new FieldInfoWriter("fields", linkCreator);
    }

    @Override
    public void write(T entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("name", entity.key());
        writer.writeField("description", entity.description());
        writer.writeList(fieldInfoWriter, entity.fields(), ctxt);
    }

}