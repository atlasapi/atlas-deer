package org.atlasapi.output.annotation;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;

import java.io.IOException;
import java.util.Optional;

public class RegionsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "regions",
            "region"
    );

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity.getChannelGroup() instanceof Platform)) {
            return;
        }

        Optional<Iterable<ChannelGroup<?>>> channelGroups = entity.getRegionChannelGroups();
        if (channelGroups.isPresent()) {
            writer.writeList(CHANNEL_GROUP_WRITER, channelGroups.get(), ctxt);
        } else {
            throw new MissingResolvedDataException(CHANNEL_GROUP_WRITER.listName());
        }

    }
}
