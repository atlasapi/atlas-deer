package org.atlasapi.system.bootstrap;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.DayRange;
import com.metabroadcast.common.time.DayRangeGenerator;

import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class SourceChannelIntervalTaskSupplier<T> implements Supplier<Iterable<T>> {

    private static final class ChannelDayScheduleFactoryIterator<T> extends AbstractIterator<T> {

        private final SourceChannelIntervalFactory<? extends T> factory;
        private final Iterator<Publisher> srcs;
        private final DayRange dayRange;
        private final Iterable<Channel> channels;

        private Iterator<LocalDate> days;
        private Iterator<Channel> chans;
        private Publisher src;
        private LocalDate day;

        public ChannelDayScheduleFactoryIterator(
                SourceChannelIntervalFactory<? extends T> taskFactory,
                ImmutableSet<Publisher> sources, DayRange dayRange, Iterable<Channel> channels) {
            this.factory = taskFactory;
            this.srcs = sources.iterator();
            this.dayRange = dayRange;
            this.channels = channels;
        }

        @Override
        protected T computeNext() {
            if (chans == null || !chans.hasNext()) {
                chans = channels.iterator();
                if (days == null || !days.hasNext()) {
                    days = dayRange.iterator();
                    if (!srcs.hasNext()) {
                        return endOfData();
                    }
                    src = srcs.next();
                }
                day = days.next();
            }
            return factory.create(src, chans.next(), interval(day));
        }

        private Interval interval(LocalDate day) {
            return new Interval(
                    day.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                    day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC)
            );
        }

    }

    private final SourceChannelIntervalFactory<? extends T> taskFactory;
    private final ChannelResolver channelResolver;
    private final DayRangeGenerator dayRangeGenerator;
    private final ImmutableSet<Publisher> sources;
    private final Clock clock;

    public SourceChannelIntervalTaskSupplier(SourceChannelIntervalFactory<? extends T> taskFactory,
            ChannelResolver channelResolver, DayRangeGenerator dayRangeGenerator,
            Iterable<Publisher> sources, Clock clock) {
        this.taskFactory = checkNotNull(taskFactory);
        this.channelResolver = checkNotNull(channelResolver);
        this.dayRangeGenerator = checkNotNull(dayRangeGenerator);
        this.sources = ImmutableSet.copyOf(sources);
        this.clock = checkNotNull(clock);
    }

    @Override
    public Iterable<T> get() {
        final DayRange dayRange = dayRangeGenerator.generate(clock.now().toLocalDate());
        final Iterable<Channel> channels;
        try {
            channels = Futures.get(
                    channelResolver.resolveChannels(ChannelQuery.builder().build()),
                    1, TimeUnit.MINUTES,
                    Exception.class
            ).getResources();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator() {
                return new ChannelDayScheduleFactoryIterator<T>(
                        taskFactory,
                        sources,
                        dayRange,
                        channels
                );
            }
        };
    }

}
