package org.atlasapi.query.v4.schedule;

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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
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
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class EquivalentScheduleResolverBackedScheduleQueryExecutorTest {


    @Mock private ApplicationEquivalentsMerger<Content> equivalentsMerger; 
    @Mock private ChannelResolver channelResolver;
    @Mock private EquivalentScheduleResolver scheduleResolver;
    
    private EquivalentScheduleResolverBackedScheduleQueryExecutor executor;
    
    @Before
    public void setup() {
        executor = new EquivalentScheduleResolverBackedScheduleQueryExecutor(channelResolver, scheduleResolver, equivalentsMerger);
    }
    
    @Test
    public void testExecutingSingleScheduleQuery() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(channel.getId()));

        EquivalentChannelSchedule channelSchedule = new EquivalentChannelSchedule(channel, interval, ImmutableList.<EquivalentScheduleEntry>of());

        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(scheduleResolver.resolveSchedules(argThat(hasItems(channel)), eq(interval), eq(query.getSource()), 
                argThat(is(ImmutableSet.of(query.getSource())))))
                .thenReturn(Futures.immediateFuture(new EquivalentSchedule(ImmutableList.of(channelSchedule), interval)));
        
        QueryResult<ChannelSchedule> result = executor.execute(query);
        
        assertThat(result.getOnlyResource(), is(new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of())));
    }
    
    @Test
    public void testExecutingMultiScheduleQuery() throws Exception {
        
        Channel channelOne = Channel.builder().build();
        channelOne.setId(1L);
        channelOne.setCanonicalUri("one");

        Channel channelTwo = Channel.builder().build();
        channelTwo.setId(2L);
        channelTwo.setCanonicalUri("two");
        
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        List<Id> cids = ImmutableList.of(Id.valueOf(channelOne.getId()), Id.valueOf(channelTwo.getId()));
        ScheduleQuery query = ScheduleQuery.multi(METABROADCAST, interval, QueryContext.standard(), cids);

        EquivalentChannelSchedule cs1 = new EquivalentChannelSchedule(channelOne, interval, ImmutableList.<EquivalentScheduleEntry>of());
        EquivalentChannelSchedule cs2 = new EquivalentChannelSchedule(channelTwo, interval, ImmutableList.<EquivalentScheduleEntry>of());

        when(channelResolver.forIds(Lists.transform(cids, Id.toLongValue())))
            .thenReturn(ImmutableList.of(channelOne, channelTwo));
        when(scheduleResolver.resolveSchedules(argThat(hasItems(channelOne, channelTwo)), eq(interval), eq(query.getSource()), 
                argThat(is(ImmutableSet.of(query.getSource())))))
            .thenReturn(Futures.immediateFuture(new EquivalentSchedule(ImmutableList.of(cs1, cs2), interval)));
        
        QueryResult<ChannelSchedule> result = executor.execute(query);
        
        assertThat(result.getResources().toList(), is(ImmutableList.of(
                new ChannelSchedule(channelOne, interval, ImmutableList.<ItemAndBroadcast>of()),
                new ChannelSchedule(channelTwo, interval, ImmutableList.<ItemAndBroadcast>of())
        )));
    }
    
    
    @Test
    public void testThrowsExceptionIfChannelIsMissing() {
        
        when(channelResolver.fromId(any(Long.class)))
            .thenReturn(Maybe.<Channel>nothing());

        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(1));
        
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
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        List<SourceReadEntry> reads = ImmutableList.copyOf(Iterables.transform(Publisher.all(), new Function<Publisher, SourceReadEntry>() {
           @Override
            public SourceReadEntry apply(@Nullable Publisher input) {
                return new SourceReadEntry(input, SourceStatus.AVAILABLE_ENABLED);
            }}));
        
        ApplicationSources appSources = ApplicationSources.defaults().copy()
                .withPrecedence(true)
                .withReadableSources(reads)
                .build();
        QueryContext context = new QueryContext(appSources, ActiveAnnotations.standard());
        
        Id itemId = Id.valueOf(1);
        Item scheduleItem = new Item(itemId, METABROADCAST);
        Item equivalentItem = new Item(Id.valueOf(2), METABROADCAST);

        scheduleItem.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        EquivalentChannelSchedule channelSchedule = new EquivalentChannelSchedule(channel, interval, 
                ImmutableList.of(
            new EquivalentScheduleEntry(
                new Broadcast(channel, interval),
                new Equivalent<Item>(EquivalenceGraph.valueOf(scheduleItem.toRef()), 
                        ImmutableList.of(scheduleItem, equivalentItem))
            )
        ));

        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, context, Id.valueOf(channel.getId()));

        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(scheduleResolver.resolveSchedules(argThat(hasItems(channel)), eq(interval), eq(query.getSource()), 
        argThat(is(query.getContext().getApplicationSources().getEnabledReadSources()))))
            .thenReturn(Futures.immediateFuture(new EquivalentSchedule(ImmutableList.of(channelSchedule), interval)));
        when(equivalentsMerger.merge(ImmutableSet.of(scheduleItem, equivalentItem), appSources))
            .thenReturn(ImmutableList.of(equivalentItem));
        
        QueryResult<ChannelSchedule> result = executor.execute(query);
        
        assertThat(result.getOnlyResource().getEntries().get(0).getItem(), sameInstance(equivalentItem));
        verify(equivalentsMerger).merge(ImmutableSet.of(scheduleItem, equivalentItem), appSources);
        
    }
    
}
