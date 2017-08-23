package org.atlasapi.query.v4.topic;

import java.io.IOException;
import java.util.List;

import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;
import org.atlasapi.topic.Topic;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopicListWriter implements EntityListWriter<ResolvedContent> {

    private final AnnotationRegistry<Topic, ResolvedContent> annotationRegistry;

    public TopicListWriter(AnnotationRegistry<Topic, ResolvedContent> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.TOPIC);
        List<OutputAnnotation<? super Topic, ResolvedContent>> annotations = ctxt
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
    public String fieldName(ResolvedContent entity) {
        return "topic";
    }

}
