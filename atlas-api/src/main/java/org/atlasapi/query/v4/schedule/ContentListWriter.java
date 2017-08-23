package org.atlasapi.query.v4.schedule;

import java.io.IOException;
import java.util.List;

import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ContentListWriter implements EntityListWriter<ResolvedContent> {

    private AnnotationRegistry<Content, ResolvedContent> annotationRegistry;

    public ContentListWriter(AnnotationRegistry<Content, ResolvedContent> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.CONTENT);
        List<OutputAnnotation<? super Content, ResolvedContent>> annotations = ctxt
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
    public String fieldName(ResolvedContent entity) {
        return entity.getClass().getSimpleName().toLowerCase();
    }
}