package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.event.Event;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.Resource;
import org.atlasapi.topic.Topic;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventDetailsAnnotation extends OutputAnnotation<Event, ResolvedContent> {

    private final EntityListWriter<Topic> topicListWriter = getTopicListWriter();
    private final AnnotationRegistry<Topic, ResolvedContent> annotationRegistry;

    public EventDetailsAnnotation(AnnotationRegistry<Topic, ResolvedContent> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {

        writer.writeObject(topicListWriter, entity.getEvent().getVenue(), ctxt);
        writer.writeList(topicListWriter, entity.getEvent().getEventGroups(), ctxt);
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
                List<OutputAnnotation<? super Topic, ResolvedContent>> annotations = ctxt
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
