package org.atlasapi.query.v4.channelgroup;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupListWriter implements EntityListWriter<ResolvedChannelGroup> { //new writer implement EntityWriter

    private final AnnotationRegistry<ResolvedChannelGroup> annotationRegistry;

    public ChannelGroupListWriter(AnnotationRegistry<ResolvedChannelGroup> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Nonnull
    @Override
    public String listName() {
        return "channel_groups";
    }

    @Override
    public void write(@Nonnull ResolvedChannelGroup entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.CHANNEL_GROUP);
        List<OutputAnnotation<? super ResolvedChannelGroup>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (OutputAnnotation<? super ResolvedChannelGroup> annotation : annotations) {
            annotation.write(entity, writer, ctxt);
        }
        ctxt.endResource();
    }

    @Nonnull
    @Override
    public String fieldName(ResolvedChannelGroup entity) {
        return "channel_group";
    }
}
