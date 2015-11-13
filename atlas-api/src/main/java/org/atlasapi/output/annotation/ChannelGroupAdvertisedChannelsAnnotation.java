package org.atlasapi.output.annotation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import org.joda.time.LocalDate;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;

public class ChannelGroupAdvertisedChannelsAnnotation extends OutputAnnotation<ChannelGroup<?>> {

    private final ChannelGroupChannelWriter channelWriter;
    private final ChannelResolver channelResolver;

    public ChannelGroupAdvertisedChannelsAnnotation(ChannelGroupChannelWriter channelWriter, ChannelResolver channelResolver) {
        this.channelWriter = checkNotNull(channelWriter);
        this.channelResolver = checkNotNull(channelResolver);
    }

    @Override
    public void write(ChannelGroup<?> entity, FieldWriter writer, OutputContext ctxt) throws
            IOException {
        final ImmutableMultimap.Builder<Id, ChannelGroupMembership> builder = ImmutableMultimap.builder();
        Iterable<? extends ChannelGroupMembership> availableChannels = entity.getChannelsAvailable(
                LocalDate.now());
        List<Id> orderedIds = StreamSupport.stream(availableChannels.spliterator(), false)
                //TODO fix channel appearing twice in ordering blowing this thing up
                .map(cm -> cm.getChannel().getId())
                .distinct()
                .collect(Collectors.toList());
        Ordering<Id> idOrdering = Ordering.explicit(orderedIds);
        for (ChannelGroupMembership channelGroupMembership : availableChannels) {
            builder.put(channelGroupMembership.getChannel().getId(), channelGroupMembership);
        }

        ImmutableMultimap<Id, ChannelGroupMembership> channelGroupMemberships = builder.build();
        String genre = ctxt.getRequest().getParameter(Attributes.CHANNEL_GROUP_CHANNEL_GENRES.externalName());

        Iterable<Channel> channels = Futures.get(
                Futures.transform(
                        this.channelResolver.resolveIds(channelGroupMemberships.keySet()),
                        (Resolved<Channel> channelResolved) -> {
                            return StreamSupport.stream(channelResolved.getResources().spliterator(), false)
                                    .filter(channel -> channel.getAdvertiseFrom().isBeforeNow() || channel.getAdvertiseFrom().isEqualNow())
                                    .sorted((o1, o2) -> idOrdering.compare(o1.getId(), o2.getId()))
                                    .collect(Collectors.toList());
                        }

                ), 1, TimeUnit.MINUTES, IOException.class
        );
        if (!Strings.isNullOrEmpty(genre)) {
            final ImmutableSet<String> genres = ImmutableSet.copyOf(Splitter.on(',').split(genre));
            channels = Iterables.filter(channels, new Predicate<Channel>() {
                @Override
                public boolean apply(Channel input) {
                    return !Sets.intersection(input.getGenres(), genres).isEmpty();
                }
            });
        }
        ImmutableSet.Builder<ChannelWithChannelGroupMembership> resultBuilder = ImmutableSet.builder();

        for (Channel channel : channels) {
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
