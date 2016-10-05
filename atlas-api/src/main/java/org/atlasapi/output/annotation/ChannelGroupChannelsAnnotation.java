package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.output.ChannelWithChannelGroupMembership;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.MissingResolvedDataException;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupChannelsAnnotation extends OutputAnnotation<ResolvedChannelGroup> {

    private final ChannelGroupChannelWriter channelWriter;

    public ChannelGroupChannelsAnnotation(ChannelGroupChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {

        Optional<Iterable<Channel>> resolvedChannels = entity.getChannels();

        if(!resolvedChannels.isPresent()) {
            throw new MissingResolvedDataException(channelWriter.listName());
        }

        Iterable<Channel> channels = resolvedChannels.get();

        String genre = ctxt.getRequest()
                .getParameter(Attributes.CHANNEL_GROUP_CHANNEL_GENRES.externalName());

        if (!Strings.isNullOrEmpty(genre)) {
            final ImmutableSet<String> genres = ImmutableSet.copyOf(Splitter.on(',')
                    .split(genre));
            channels = Iterables.filter(channels,
                    input -> !Sets.intersection(input.getGenres(), genres).isEmpty()
            );
        }

        ImmutableSet.Builder<ChannelWithChannelGroupMembership> resultBuilder = ImmutableSet.builder();

        ImmutableMultimap.Builder<Id, ChannelGroupMembership> builder = ImmutableMultimap.builder();
        for (ChannelGroupMembership channelGroupMembership :
                entity.getChannelGroup().getChannelsAvailable(LocalDate.now())) {
            builder.put(channelGroupMembership.getChannel().getId(), channelGroupMembership);
        }
        ImmutableMultimap<Id, ChannelGroupMembership> channelGroupMemberships = builder.build();

        for (Channel channel : channels) {
            for (ChannelGroupMembership channelGroupMembership : channelGroupMemberships.get(
                    channel.getId())) {
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
