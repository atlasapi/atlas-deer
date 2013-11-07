package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.atlasapi.content.Content;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

public final class ContentListWriter implements EntityListWriter<Content> {

    private AnnotationRegistry<Content> annotationRegistry;

    public ContentListWriter(AnnotationRegistry<Content> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }
    
    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.CONTENT);
        List<OutputAnnotation<? super Content>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Override
    public String listName() {
        return "content";
    }

    @Override
    public String fieldName(Content entity) {
        return entity.getClass().getSimpleName().toLowerCase();
    }
}