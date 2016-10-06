package org.atlasapi.query.v4.channel;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelListWriter implements EntityListWriter<ResolvedChannel> {

    private AnnotationRegistry<ResolvedChannel> annotationRegistry;

    public ChannelListWriter(AnnotationRegistry<ResolvedChannel> annotationRegistry) {
        this.annotationRegistry = checkNotNull(annotationRegistry);
    }

    @Override
    public String listName() {
        return "channels";
    }

    @Override
    public void write(@Nonnull ResolvedChannel entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        ctxt.startResource(Resource.CHANNEL);
        List<OutputAnnotation<? super ResolvedChannel>> annotations = ctxt
                .getAnnotations(annotationRegistry);
        for (OutputAnnotation<? super ResolvedChannel> annotation : annotations) {
            annotation.write(entity, writer, ctxt);
        }
        ctxt.endResource();

    }

    @Nonnull
    @Override
    public String fieldName(ResolvedChannel entity) {
        return entity.getChannel().getClass().getSimpleName().toLowerCase();
    }

}
