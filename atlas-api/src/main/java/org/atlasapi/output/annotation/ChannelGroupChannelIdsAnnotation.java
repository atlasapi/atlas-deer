package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupChannelIdsWriter;

import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelIdsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private final ChannelGroupChannelIdsWriter channelIdsWriter;

    public ChannelGroupChannelIdsAnnotation(ChannelGroupChannelIdsWriter channelIdsWriter) {
        this.channelIdsWriter = checkNotNull(channelIdsWriter);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        boolean lcnSharing = ctxt.getActiveAnnotations().contains(Annotation.LCN_SHARING);
        ImmutableList<ChannelGroupMembership> channels = ImmutableList.copyOf(
                entity.getChannelGroup().getChannelsAvailable(LocalDate.now(), lcnSharing)
        );
        writer.writeList(channelIdsWriter, channels, ctxt);
    }
}
