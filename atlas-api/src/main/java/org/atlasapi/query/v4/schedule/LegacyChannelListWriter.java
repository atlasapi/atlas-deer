package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

public final class LegacyChannelListWriter implements EntityListWriter<Channel> {

    private AnnotationRegistry<Channel> annotationRegistry;

    public LegacyChannelListWriter(AnnotationRegistry<Channel> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }
    
    @Override
    public void write(Channel entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        ctxt.startResource(Resource.CHANNEL);
        List<OutputAnnotation<? super Channel>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (int i = 0; i < annotations.size(); i++) {
            annotations.get(i).write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Override
    public String fieldName(Channel entity) {
        return "channel";
    }
    
    @Override
    @Nonnull
    public String listName() {
        return "channels";
    }
    
}