package org.atlasapi.system.legacy;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleResolver;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class LegacyScheduleResolver implements ScheduleResolver {

    private final org.atlasapi.persistence.content.ScheduleResolver legacyResolver;
    private final LegacyContentTransformer transformer;
    private final LegacyChannelTransformer channelTransformer;

    public LegacyScheduleResolver(org.atlasapi.persistence.content.ScheduleResolver legacyResolver,
            LegacyContentTransformer legacyContentTransformer) {
        this.legacyResolver = checkNotNull(legacyResolver);
        this.transformer = checkNotNull(legacyContentTransformer);
        this.channelTransformer = new LegacyChannelTransformer();
    }

    @Override
    public ListenableFuture<Schedule> resolve(Iterable<Channel> channels, Interval interval,
            Publisher source) {
        org.atlasapi.media.entity.Schedule schedule = resolveLegacy(channels, interval, source);
        return Futures.immediateFuture(transform(schedule));
    }

    private Schedule transform(org.atlasapi.media.entity.Schedule schedule) {
        return new Schedule(
                transform(schedule.scheduleChannels(), schedule.interval()),
                schedule.interval()
        );
    }

    private List<ChannelSchedule> transform(List<ScheduleChannel> scheduleChannels,
            final Interval interval) {
        return ImmutableList.copyOf(Lists.transform(
                scheduleChannels,
                new Function<ScheduleChannel, ChannelSchedule>() {

                    @Override
                    public ChannelSchedule apply(ScheduleChannel input) {
                        return new ChannelSchedule(
                                channelTransformer.apply(input.channel()),
                                interval,
                                toIabs(input.items(), channelTransformer.apply(input.channel()))
                        );
                    }
                }
        ));
    }

    private Iterable<ItemAndBroadcast> toIabs(List<org.atlasapi.media.entity.Item> items,
            final Channel channel) {
        return Lists.transform(
                items,
                new Function<org.atlasapi.media.entity.Item, ItemAndBroadcast>() {

                    @Override
                    public ItemAndBroadcast apply(org.atlasapi.media.entity.Item input) {
                        Item item = (Item) transformer.apply(input);
                        Broadcast broadcast = onlyBroadcastFrom(item);
                        checkState(channel.getId().equals(broadcast.getChannelId()),
                                "%s not on expected channel %s", broadcast, channel
                        );
                        return new ItemAndBroadcast(item, broadcast);
                    }

                }
        );
    }

    private Broadcast onlyBroadcastFrom(Item item) {
        return Iterables.getOnlyElement(item.getBroadcasts());
    }

    private org.atlasapi.media.entity.Schedule resolveLegacy(Iterable<Channel> channels,
            Interval interval, Publisher source) {

        Iterable<org.atlasapi.media.channel.Channel> legacyChannels = Iterables.transform(
                channels,
                new Function<Channel, org.atlasapi.media.channel.Channel>() {

                    @Nullable
                    @Override
                    public org.atlasapi.media.channel.Channel apply(Channel input) {
                        return channelTransformer.toBasicLegacyChannel(input);
                    }
                }
        );
        return legacyResolver.unmergedSchedule(
                interval.getStart(),
                interval.getEnd(),
                legacyChannels,
                ImmutableSet.of(source)
        );
    }

}
