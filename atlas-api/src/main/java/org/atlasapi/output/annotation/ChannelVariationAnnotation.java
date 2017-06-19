package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;
import org.atlasapi.query.v4.channel.ChannelWriter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelVariationAnnotation extends OutputAnnotation<ResolvedChannel> {

    private final ChannelWriter channelWriter;

    public ChannelVariationAnnotation(
            ChannelWriter channelWriter
    ) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ResolvedChannel entity, FieldWriter format, OutputContext ctxt) throws IOException {

        Optional<Iterable<Channel>> channelVariations = entity.getChannelVariations();
        if (!channelVariations.isPresent()) {
            throw new MissingResolvedDataException("channel variations");
        }

        // Channels must be wrapped in a ResolvedChannel because of how the current writers work.
        // The current writers have been changed to write ResolvedChannel so when one of the
        // resolved channel fields (of type Channel) are pulled and sent to a writer, the types
        // stop matching (ResolvedChannel != Channel)
        Iterable<ResolvedChannel> resolvedChannels =
                StreamSupport.stream(channelVariations.get().spliterator(), false)
                .map(input -> ResolvedChannel.builder(input).build())
                .collect(Collectors.toList());

        format.writeList(channelWriter, resolvedChannels, ctxt);
    }
}
