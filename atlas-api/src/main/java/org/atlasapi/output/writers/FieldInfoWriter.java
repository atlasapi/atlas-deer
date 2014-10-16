package org.atlasapi.output.writers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.generation.model.FieldInfo;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.meta.LinkCreator;


public class FieldInfoWriter implements EntityListWriter<FieldInfo> {
    
    private static final String ELEMENT_NAME = "field";
    
    private final String listName;
    private final LinkCreator linkCreator;
    
    public FieldInfoWriter(String listName, LinkCreator linkCreator) {
        this.listName = checkNotNull(listName);
        this.linkCreator = checkNotNull(linkCreator);
    }

    @Override
    public void write(FieldInfo entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("name", entity.name());
        writer.writeField("description", entity.description());
        writer.writeField("type", entity.type());
        if (entity.isModelType()) {
            writer.writeField("model_class_link",linkCreator.createModelLink(entity.type().toLowerCase()));
      }
        writer.writeField("json_type", entity.jsonType().value());
    }

    @Override
    public String fieldName(FieldInfo entity) {
        return ELEMENT_NAME;
    }

    @Override
    public String listName() {
        return listName;
    }
}
