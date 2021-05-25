package org.atlasapi.output.annotation;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.NumberedChannelGroup;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupWriter;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;

import java.io.IOException;
import java.util.Optional;

public class BaseChannelGroupsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private static final ChannelGroupWriter CHANNEL_GROUP_WRITER = new ChannelGroupWriter(
            "channel_numbers_from",
            "channel_numbers_from"
    );

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        if (!(entity.getChannelGroup() instanceof NumberedChannelGroup)) {
            return;
        }
        Optional<ChannelGroup<?>> channelGroup = entity.getChannelNumbersFromGroup();
        writer.writeObject(CHANNEL_GROUP_WRITER, channelGroup.orElse(null), ctxt);
    }
}
