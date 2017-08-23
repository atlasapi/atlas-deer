package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResolvedChannelWithChannelGroupMembership;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;
import org.atlasapi.query.v4.channelgroup.ChannelGroupChannelWriter;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupAdvertisedChannelsAnnotation extends OutputAnnotation<ResolvedChannelGroup, ResolvedChannelGroup> {

    private final ChannelGroupChannelWriter channelWriter;

    public ChannelGroupAdvertisedChannelsAnnotation(ChannelGroupChannelWriter channelWriter) {
        this.channelWriter = checkNotNull(channelWriter);
    }

    @Override
    public void write(ResolvedChannelGroup entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {

        Optional<Iterable<ResolvedChannel>> resolvedChannels = entity.getChannels();
        if (!resolvedChannels.isPresent()) {
            throw new MissingResolvedDataException("channel group advertised channels annotation");
        }

        Iterable<ResolvedChannel> filteredChannels = StreamSupport.stream(resolvedChannels.get().spliterator(), false)
                .filter(resolvedChannel -> resolvedChannel.getChannel().getAdvertiseFrom().isBeforeNow()
                        || resolvedChannel.getChannel().getAdvertiseFrom().isEqualNow())
                .collect(Collectors.toList());

        String genre = ctxt.getRequest()
                .getParameter(Attributes.CHANNEL_GROUP_CHANNEL_GENRES.externalName());

        if (!Strings.isNullOrEmpty(genre)) {
            final ImmutableSet<String> genres = ImmutableSet.copyOf(Splitter.on(',').split(genre));
            filteredChannels = Iterables.filter(filteredChannels,
                    input -> !Sets.intersection(input.getChannel().getGenres(), genres).isEmpty()
            );
        }
        ImmutableSet.Builder<ResolvedChannelWithChannelGroupMembership> resultBuilder = ImmutableSet.builder();

        ImmutableMultimap.Builder<Id, ChannelGroupMembership> builder = ImmutableMultimap.builder();
        for (ChannelGroupMembership channelGroupMembership :
                entity.getChannelGroup().getChannelsAvailable(LocalDate.now())) {
            builder.put(channelGroupMembership.getChannel().getId(), channelGroupMembership);
        }
        ImmutableMultimap<Id, ChannelGroupMembership> channelGroupMemberships = builder.build();

        for (ResolvedChannel channel : filteredChannels) {
            for (ChannelGroupMembership channelGroupMembership : channelGroupMemberships.get(channel
                    .getChannel()
                    .getId())) {
                resultBuilder.add(
                        new ResolvedChannelWithChannelGroupMembership(
                                channel,
                                channelGroupMembership
                        )
                );
            }
        }

        ImmutableSet<ResolvedChannelWithChannelGroupMembership> result = resultBuilder.build();

        writer.writeList(channelWriter,result,ctxt);
    }

}
