package org.atlasapi.query.v4.channelgroup;

import org.atlasapi.channel.ChannelGroup;
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

public class ChannelGroupListWriter implements EntityListWriter<ChannelGroup<?>> {
    private final AnnotationRegistry<ChannelGroup<?>> annotationRegistry;

    public ChannelGroupListWriter(AnnotationRegistry<ChannelGroup<?>> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Nonnull
    @Override
    public String listName() {
        return "channel_groups";
    }

    @Override
    public void write(@Nonnull ChannelGroup<?> entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.CHANNEL_GROUP);
        List<OutputAnnotation<? super ChannelGroup<?>>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (OutputAnnotation<? super ChannelGroup<?>> annotation : annotations) {
            annotation.write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Nonnull
    @Override
    public String fieldName(ChannelGroup entity) {
        return "channel_group";
    }
}
