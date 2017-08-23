package org.atlasapi.query.v4.event;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.event.Event;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventListWriter implements EntityListWriter<ResolvedContent> {

    private final AnnotationRegistry<Event, ResolvedContent> annotationRegistry;

    public EventListWriter(AnnotationRegistry<Event, ResolvedContent> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public String listName() {
        return "events";
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.EVENT);
        List<OutputAnnotation<? super Event, ResolvedContent>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Nonnull
    @Override
    public String fieldName(ResolvedContent entity) {
        return "event";
    }
}
