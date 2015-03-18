package org.atlasapi.output.annotation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.ChannelWithChannelGroupMembership;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelsAnnotation extends OutputAnnotation<org.atlasapi.channel.ChannelGroup> {

    private final ChannelGroupChannelWriter channelWriter;
    private final ChannelResolver channelResolver;

    public ChannelGroupChannelsAnnotation(ChannelGroupChannelWriter channelWriter, ChannelResolver channelResolver) {
        this.channelWriter = checkNotNull(channelWriter);
        this.channelResolver = checkNotNull(channelResolver);
    }

    @Override
    public void write(ChannelGroup entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        final ImmutableMultimap.Builder<Id, ChannelGroupMembership> builder = ImmutableMultimap.builder();
        for (ChannelGroupMembership channelGroupMembership : (Set<ChannelGroupMembership>)entity.getChannels()) {
            builder.put(channelGroupMembership.getChannel().getId(), channelGroupMembership);
        }

        ImmutableMultimap<Id, ChannelGroupMembership> channelGroupMemberships = builder.build();
        Iterable<Channel> channels = Futures.get(
                Futures.transform(
                        this.channelResolver.resolveIds(channelGroupMemberships.keySet()),
                        new Function<Resolved<Channel>, Iterable<Channel>>() {
                            @Override
                            public Iterable<Channel> apply(Resolved<Channel> channelResolved) {
                                return channelResolved.getResources();
                            }
                        }

                ), 1, TimeUnit.MINUTES, IOException.class
        );
        ImmutableSet.Builder<ChannelWithChannelGroupMembership> resultBuilder = ImmutableSet.builder();
        String genre = ctxt.getRequest().getParameter(Attributes.CHANNEL_GROUP_CHANNEL_GENRE.externalName());
        for (Channel channel : channels) {
            if (genre != null && !channel.getGenres().contains(genre)) {
                continue;
            }
            for (ChannelGroupMembership channelGroupMembership : channelGroupMemberships.get(channel.getId())) {
                resultBuilder.add(
                        new ChannelWithChannelGroupMembership(
                                channel,
                                channelGroupMembership
                        )
                );
            }
        }
        ImmutableSet<ChannelWithChannelGroupMembership> result = resultBuilder.build();
        writer.writeList(channelWriter, result, ctxt);
    }
}
