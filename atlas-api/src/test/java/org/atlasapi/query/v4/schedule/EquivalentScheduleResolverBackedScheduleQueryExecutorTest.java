package org.atlasapi.query.v4.schedule;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.EquivalentChannelSchedule;
import org.atlasapi.schedule.EquivalentSchedule;
import org.atlasapi.schedule.EquivalentScheduleEntry;
import org.atlasapi.schedule.EquivalentScheduleResolver;
import org.atlasapi.schedule.FlexibleBroadcastMatcher;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EquivalentScheduleResolverBackedScheduleQueryExecutorTest {

    @Mock private ApplicationEquivalentsMerger<Content> equivalentsMerger;
    @Mock private ChannelResolver channelResolver;
    @Mock private EquivalentScheduleResolver scheduleResolver;
    @Mock private FlexibleBroadcastMatcher broadcastMatcher;

    private EquivalentScheduleResolverBackedScheduleQueryExecutor executor;

    @Before
    public void setup() {
        executor = new EquivalentScheduleResolverBackedScheduleQueryExecutor(
                channelResolver,
                scheduleResolver,
                equivalentsMerger,
                broadcastMatcher
        );
    }

    @Test
    public void testExecutingSingleScheduleQuery() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);
        Interval interval = new Interval(start, end);
        ScheduleQuery query = ScheduleQuery.single(
                METABROADCAST,
                start,
                end,
                QueryContext.standard(mock(HttpServletRequest.class)),
                channel.getId()
        );

        EquivalentChannelSchedule channelSchedule = new EquivalentChannelSchedule(
                channel,
                interval,
                ImmutableList.<EquivalentScheduleEntry>of()
        );

        when(channelResolver.resolveIds(ImmutableSet.of(channel.getId())))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(channel))));
        when(scheduleResolver.resolveSchedules(argThat(hasItems(channel)),
                eq(interval),
                eq(query.getSource()),
                argThat(is(ImmutableSet.of(query.getSource())))
        ))
                .thenReturn(Futures.immediateFuture(new EquivalentSchedule(ImmutableList.of(
                        channelSchedule), interval)));

        QueryResult<ChannelSchedule> result = executor.execute(query);

        assertThat(
                result.getOnlyResource(),
                is(new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of()))
        );
    }

    @Test
    public void testExecutingMultiScheduleQuery() throws Exception {

        Channel channelOne = Channel.builder(Publisher.BBC).build();
        channelOne.setId(1L);
        channelOne.setCanonicalUri("one");

        Channel channelTwo = Channel.builder(Publisher.BBC).build();
        channelTwo.setId(2L);
        channelTwo.setCanonicalUri("two");

        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);
        Interval interval = new Interval(start, end);
        List<Id> cids = ImmutableList.of(channelOne.getId(), channelTwo.getId());
        ScheduleQuery query = ScheduleQuery.multi(
                METABROADCAST,
                start,
                end,
                QueryContext.standard(mock(HttpServletRequest.class)),
                cids
        );

        EquivalentChannelSchedule cs1 = new EquivalentChannelSchedule(
                channelOne,
                interval,
                ImmutableList.<EquivalentScheduleEntry>of()
        );
        EquivalentChannelSchedule cs2 = new EquivalentChannelSchedule(
                channelTwo,
                interval,
                ImmutableList.<EquivalentScheduleEntry>of()
        );

        when(channelResolver.resolveIds(argThat(org.hamcrest.Matchers.<Id>iterableWithSize(2))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(
                        channelOne,
                        channelTwo
                ))));
        when(scheduleResolver.resolveSchedules(argThat(hasItems(channelOne, channelTwo)),
                eq(interval),
                eq(query.getSource()),
                argThat(is(ImmutableSet.of(query.getSource())))
        ))
                .thenReturn(Futures.immediateFuture(new EquivalentSchedule(ImmutableList.of(
                        cs1,
                        cs2
                ), interval)));

        QueryResult<ChannelSchedule> result = executor.execute(query);

        assertThat(result.getResources().toList(), is(ImmutableList.of(
                new ChannelSchedule(channelOne, interval, ImmutableList.<ItemAndBroadcast>of()),
                new ChannelSchedule(channelTwo, interval, ImmutableList.<ItemAndBroadcast>of())
        )));
    }

    @Test
    public void testThrowsExceptionIfChannelIsMissing() {

        when(channelResolver.resolveIds(any(List.class)))
                .thenReturn(Futures.immediateFuture(Resolved.<Channel>empty()));

        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);
        Interval interval = new Interval(start, end);
        ScheduleQuery query = ScheduleQuery.single(
                METABROADCAST,
                start,
                end,
                QueryContext.standard(mock(HttpServletRequest.class)),
                Id.valueOf(1)
        );

        try {
            executor.execute(query);
            fail("expected NotFoundException");
        } catch (QueryExecutionException qee) {
            assertThat(qee, is(instanceOf(NotFoundException.class)));
            verifyZeroInteractions(scheduleResolver);
        }
    }

    @Test
    public void testResolvesEquivalentContentForApiKeyWithPrecedenceEnabled() throws Exception {
        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Channel channel2 = Channel.builder(Publisher.BBC).build();
        channel2.setId(2L);
        channel2.setCanonicalUri("two");
        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);
        Interval interval = new Interval(start, end);
        List<SourceReadEntry> reads = ImmutableList.copyOf(Iterables.transform(
                Publisher.all(),
                new Function<Publisher, SourceReadEntry>() {

                    @Override
                    public SourceReadEntry apply(@Nullable Publisher input) {
                        return new SourceReadEntry(input, SourceStatus.AVAILABLE_ENABLED);
                    }
                }
        ));

        ApplicationSources appSources = ApplicationSources.defaults().copy()
                .withPrecedence(true)
                .withReadableSources(reads)
                .build();
        QueryContext context = new QueryContext(
                appSources,
                ActiveAnnotations.standard(),
                mock(HttpServletRequest.class)
        );

        Id itemId = Id.valueOf(1);
        Item scheduleItem = new Item(itemId, METABROADCAST);
        Item equivalentItem = new Item(Id.valueOf(2), METABROADCAST);

        Broadcast targetBroadcast = new Broadcast(channel, interval);
        Set<Broadcast> equivalentBroadcasts = ImmutableSet.of(
                targetBroadcast,
                new Broadcast(channel2, interval)
        );
        equivalentItem.setBroadcasts(equivalentBroadcasts);

        scheduleItem.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast originalBroadcast = new Broadcast(channel, interval);
        EquivalentChannelSchedule channelSchedule = new EquivalentChannelSchedule(channel, interval,
                ImmutableList.of(
                        new EquivalentScheduleEntry(
                                originalBroadcast,
                                new Equivalent<Item>(
                                        EquivalenceGraph.valueOf(scheduleItem.toRef()),
                                        ImmutableList.of(scheduleItem, equivalentItem)
                                )
                        )
                )
        );

        ScheduleQuery query = ScheduleQuery.single(
                METABROADCAST,
                start,
                end,
                context,
                channel.getId()
        );

        when(channelResolver.resolveIds(argThat(org.hamcrest.Matchers.<Id>iterableWithSize(1))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(channel))));
        when(scheduleResolver.resolveSchedules(argThat(hasItems(channel)),
                eq(interval),
                eq(query.getSource()),
                argThat(is(query.getContext().getApplicationSources().getEnabledReadSources()))
        ))
                .thenReturn(Futures.immediateFuture(new EquivalentSchedule(ImmutableList.of(
                        channelSchedule), interval)));
        when(equivalentsMerger.merge(
                Optional.<Id>absent(),
                ImmutableSet.of(scheduleItem, equivalentItem),
                appSources
        ))
                .thenReturn(ImmutableList.of(equivalentItem));
        when(broadcastMatcher.findMatchingBroadcast(originalBroadcast, equivalentBroadcasts))
                .thenReturn(Optional.of(targetBroadcast));

        QueryResult<ChannelSchedule> result = executor.execute(query);

        assertThat(
                result.getOnlyResource().getEntries().get(0).getItem(),
                sameInstance(equivalentItem)
        );
        assertThat(
                result.getOnlyResource().getEntries().get(0).getBroadcast(),
                sameInstance(targetBroadcast)
        );
        verify(equivalentsMerger).merge(
                Optional.<Id>absent(),
                ImmutableSet.of(scheduleItem, equivalentItem),
                appSources
        );

    }

}
