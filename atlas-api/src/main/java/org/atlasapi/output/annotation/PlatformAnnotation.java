package org.atlasapi.output.annotation;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Region;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;

import java.io.IOException;
import java.util.Optional;

public class PlatformAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "platforms",
            "platform"
    );

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity.getChannelGroup() instanceof Region)) {
            return;
        }

        Optional<ChannelGroup<?>> channelGroup = entity.getPlatformChannelGroup();
        if (channelGroup.isPresent()) {
            writer.writeObject(CHANNEL_GROUP_WRITER, channelGroup.get(), ctxt);
        } else {
            if (!((Region) entity.getChannelGroup()).getPlatform().isPresent()) {
                writer.writeObject(CHANNEL_GROUP_WRITER, null, ctxt);
            } else {
                throw new MissingResolvedDataException(CHANNEL_GROUP_WRITER.fieldName(entity.getChannelGroup()));
            }
        }
    }
}
