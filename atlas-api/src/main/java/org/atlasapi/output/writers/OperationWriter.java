package org.atlasapi.output.writers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.generation.model.Operation;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;


public class OperationWriter implements EntityListWriter<Operation> {
    
    private static final String ELEMENT_NAME = "operation";
    
    private final String listName;
    
    public OperationWriter(String listName) {
        this.listName = checkNotNull(listName);
    }

    @Override
    public void write(Operation entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("method", entity.method().name());
        writer.writeField("path", entity.path());
    }

    @Override
    public String fieldName(Operation entity) {
        return ELEMENT_NAME;
    }

    @Override
    public String listName() {
        return listName;
    }
}
