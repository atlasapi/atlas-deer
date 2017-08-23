package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Optional;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Region;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;

public class RegionsAnnotation extends OutputAnnotation<ResolvedChannelGroup, ResolvedChannelGroup> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "regions",
            "region"
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
            throw new MissingResolvedDataException("missing regions for channel group");
        }

    }
}
