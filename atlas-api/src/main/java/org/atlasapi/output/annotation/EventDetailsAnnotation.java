package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;

import org.atlasapi.eventV2.EventV2;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.Resource;
import org.atlasapi.topic.Topic;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventDetailsAnnotation extends OutputAnnotation<EventV2> {

    private final EntityListWriter<Topic> topicListWriter = getTopicListWriter();
    private final AnnotationRegistry<Topic> annotationRegistry;

    public EventDetailsAnnotation(AnnotationRegistry<Topic> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public void write(EventV2 entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeObject(topicListWriter, entity.getVenue(), ctxt);
        writer.writeList(topicListWriter, entity.getEventGroups(), ctxt);
    }

    private EntityListWriter<Topic> getTopicListWriter() {
        return new EntityListWriter<Topic>() {

            @Override
            public String listName() {
                return "event_groups";
            }

            @Override
            public void write(Topic entity, FieldWriter writer, OutputContext ctxt)
                    throws IOException {
                ctxt.startResource(Resource.TOPIC);
                List<OutputAnnotation<? super Topic>> annotations = ctxt
                        .getAnnotations(annotationRegistry);
                for (OutputAnnotation annotation : annotations) {
                    annotation.write(entity, writer, ctxt);
                }
                ctxt.endResource();
            }

            @Override
            public String fieldName(Topic entity) {
                return "venue";
            }
        };
    }
}
