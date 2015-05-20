package org.atlasapi.query.v4.channel;

import org.atlasapi.channel.Channel;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelListWriter implements EntityListWriter<Channel> {
    private AnnotationRegistry<Channel> annotationRegistry;

    public ChannelListWriter(AnnotationRegistry<Channel> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public String listName() {
        return "channels";
    }

    @Override
    public void write(@Nonnull Channel entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.CHANNEL);
        List<OutputAnnotation<? super Channel>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (OutputAnnotation<? super Channel> annotation : annotations) {
            annotation.write(entity, writer, ctxt);
        }
        ctxt.endResource();

    }

    @Nonnull
    @Override
    public String fieldName(Channel entity) {
        return entity.getClass().getSimpleName().toLowerCase();
    }

}
