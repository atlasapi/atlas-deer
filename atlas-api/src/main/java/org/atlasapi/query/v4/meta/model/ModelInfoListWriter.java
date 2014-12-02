package org.atlasapi.query.v4.meta.model;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

public class ModelInfoListWriter implements EntityListWriter<ModelClassInfo> {

    private final AnnotationRegistry<ModelClassInfo> annotationRegistry;

    public ModelInfoListWriter(AnnotationRegistry<ModelClassInfo> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }
    
    @Override
    public void write(ModelClassInfo entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.MODEL_INFO);
        List<OutputAnnotation<? super ModelClassInfo>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Override
    public String listName() {
        return "model_classes";
    }

    @Override
    public String fieldName(ModelClassInfo entity) {
        return "model_class";
    }

}
