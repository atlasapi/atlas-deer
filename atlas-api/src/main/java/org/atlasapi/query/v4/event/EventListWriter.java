package org.atlasapi.query.v4.event;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.eventV2.EventV2;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventListWriter implements EntityListWriter<EventV2> {

    private final AnnotationRegistry<EventV2> annotationRegistry;

    public EventListWriter(AnnotationRegistry<EventV2> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public String listName() {
        return "events";
    }

    @Override
    public void write(EventV2 entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.EVENT);
        List<OutputAnnotation<? super EventV2>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Nonnull
    @Override
    public String fieldName(EventV2 entity) {
        return "event";
    }
}
