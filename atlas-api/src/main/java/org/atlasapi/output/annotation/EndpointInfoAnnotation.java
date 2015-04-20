package org.atlasapi.output.annotation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.generation.model.Operation;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.OperationWriter;
import org.atlasapi.query.v4.meta.LinkCreator;

public class EndpointInfoAnnotation<T extends EndpointClassInfo> extends
        OutputAnnotation<T> {

    private final EntityListWriter<? super Operation> operationWriter;
    private final LinkCreator linkCreator;
    
    public EndpointInfoAnnotation(LinkCreator linkCreator) {
        this.linkCreator = checkNotNull(linkCreator);
        this.operationWriter = new OperationWriter("operations");
    }

    @Override
    public void write(T entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("name", entity.name());
        writer.writeField("model_class_link", linkCreator.createModelLink(entity.modelKey()));
        writer.writeField("description", entity.description());
        writer.writeField("root_path", entity.rootPath());
        writer.writeList(operationWriter, entity.operations(), ctxt);
    }
}