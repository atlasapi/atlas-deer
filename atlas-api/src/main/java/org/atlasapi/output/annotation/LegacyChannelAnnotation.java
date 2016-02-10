package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;
import static org.atlasapi.output.writers.SourceWriter.sourceWriter;

public class LegacyChannelAnnotation extends OutputAnnotation<Channel> {

    private static final EntityListWriter<Publisher> AVAILABLE_FROM_WRITER = sourceListWriter(
            "available_from");
    private static final EntityWriter<Publisher> BROADCASTER_WRITER = sourceWriter("broadcaster");

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        format.writeList(AVAILABLE_FROM_WRITER, entity.getAvailableFrom(), ctxt);
        format.writeObject(AVAILABLE_FROM_WRITER, entity.getSource(), ctxt);
        format.writeField("media_type", entity.getMediaType());
        format.writeObject(BROADCASTER_WRITER, entity.getBroadcaster(), ctxt);
    }

}
