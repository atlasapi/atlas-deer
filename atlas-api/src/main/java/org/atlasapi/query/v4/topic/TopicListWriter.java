package org.atlasapi.query.v4.topic;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;
import org.atlasapi.topic.Topic;

public class TopicListWriter implements EntityListWriter<Topic> {

    private final AnnotationRegistry<Topic> annotationRegistry;

    public TopicListWriter(AnnotationRegistry<Topic> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }
    
    @Override
    public void write(Topic entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.TOPIC);
        List<OutputAnnotation<? super Topic>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Override
    public String listName() {
        return "topics";
    }

    @Override
    public String fieldName(Topic entity) {
        return "topic";
    }

}
