package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.NumberedChannelGroup;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChannelGroupChannelIdsWriter;
import org.joda.time.LocalDate;

import java.io.IOException;

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
        ImmutableList<ChannelGroupMembership> availableChannels = ImmutableList.copyOf(
                entity.getChannelGroup() instanceof NumberedChannelGroup ?
                        ((NumberedChannelGroup) entity.getChannelGroup())
                                .getChannelsAvailable(LocalDate.now(), lcnSharing)
                        : entity.getChannelGroup().getChannelsAvailable(LocalDate.now())
        );
        writer.writeList(channelIdsWriter, availableChannels, ctxt);
    }
}
