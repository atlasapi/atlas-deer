package org.atlasapi.query.v4.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.event.Event;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

public class EventListWriter implements EntityListWriter<Event> {
    private final AnnotationRegistry<Event> annotationRegistry;

    public EventListWriter(AnnotationRegistry<Event> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public String listName() {
        return "events";
    }

    @Override
    public void write(Event entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.EVENT);
        List<OutputAnnotation<? super Event>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Nonnull
    @Override
    public String fieldName(Event entity) {
        return "event";
    }
}
