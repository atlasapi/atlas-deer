package org.atlasapi.system.bootstrap;

import java.util.Set;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.DayRangeGenerator;
import com.metabroadcast.common.time.TimeMachine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SourceChannelDayTaskSupplierTest {

    @Mock private SourceChannelIntervalFactory<Integer> factory;
    @Mock private ChannelResolver channelResolver;

    private DayRangeGenerator dayRangeGenerator = new DayRangeGenerator()
            .withLookAhead(1)
            .withLookBack(0);
    private Set<Publisher> srcs = ImmutableSet.of(Publisher.BBC, Publisher.PA);
    private Clock clock = new TimeMachine();

    private SourceChannelIntervalTaskSupplier<Integer> supplier;

    @Before
    public void setup() {
        supplier = new SourceChannelIntervalTaskSupplier<Integer>(
                factory,
                channelResolver,
                dayRangeGenerator,
                srcs,
                clock
        );
    }

    @Test
    public void testSuppliesTasksForAllSrcDayChannels() {

        Channel channel1 = Channel.builder(Publisher.BBC).withUri("channel1").build();
        Channel channel2 = Channel.builder(Publisher.BBC).withUri("channel2").build();

        when(channelResolver.resolveChannels(any(ChannelQuery.class)))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(
                        channel1,
                        channel2
                ))));
        when(factory.create(any(Publisher.class), any(Channel.class), any(Interval.class)))
                .thenReturn(1);

        ImmutableList<Integer> numbers = ImmutableList.copyOf(supplier.get());

        assertThat(numbers.size(), is(8));

        verify(factory).create(Publisher.BBC, channel1, interval(clock.now().toLocalDate()));
        verify(factory).create(
                Publisher.BBC,
                channel1,
                interval(clock.now().toLocalDate().plusDays(1))
        );
        verify(factory).create(Publisher.BBC, channel2, interval(clock.now().toLocalDate()));
        verify(factory).create(
                Publisher.BBC,
                channel2,
                interval(clock.now().toLocalDate().plusDays(1))
        );
        verify(factory).create(Publisher.PA, channel1, interval(clock.now().toLocalDate()));
        verify(factory).create(
                Publisher.PA,
                channel1,
                interval(clock.now().toLocalDate().plusDays(1))
        );
        verify(factory).create(Publisher.PA, channel2, interval(clock.now().toLocalDate()));
        verify(factory).create(
                Publisher.PA,
                channel2,
                interval(clock.now().toLocalDate().plusDays(1))
        );

    }

    private Interval interval(LocalDate day) {
        return new Interval(
                day.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC)
        );
    }

}
