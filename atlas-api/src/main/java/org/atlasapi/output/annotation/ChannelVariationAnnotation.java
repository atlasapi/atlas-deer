package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.MissingResolvedDataException;
import org.atlasapi.query.v4.channel.ChannelWriter;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import scala.concurrent.pilib;

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
        if (channelVariations.isPresent()) {
            Iterable<ResolvedChannel> resolvedChannels =
                    StreamSupport.stream(channelVariations.get().spliterator(), false)
                    .map(input -> ResolvedChannel.builder(input).build())
                    .collect(Collectors.toList());
            // ^^ This is because the writer needs a ResolvedChannel as it does an annotation check
            // for some reason so channels need to be converted to resolved channels before theyre
            // written

            format.writeList(channelWriter, resolvedChannels, ctxt);
        } else {
            throw new MissingResolvedDataException("channel variations");
        }
    }
}
