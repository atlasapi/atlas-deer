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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

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
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleResolverBackedScheduleQueryExecutorTest {

    @Mock private MergingEquivalentsResolver<Content> equivalentContentResolver; 
    @Mock private ChannelResolver channelResolver;
    @Mock private ScheduleResolver scheduleResolver;
    
    private ScheduleResolverBackedScheduleQueryExecutor executor;
    
    @Before
    public void setup() {
        executor = new ScheduleResolverBackedScheduleQueryExecutor(channelResolver, scheduleResolver, equivalentContentResolver);
    }
    
    @Test
    public void testExecutingSingleScheduleQuery() throws Exception {
        
//        Channel channel = Channel.builder(Publisher.BBC).build();
//        channel.setId(1L);
//        channel.setCanonicalUri("one");
//        DateTime start = new DateTime(0, DateTimeZones.UTC);
//        DateTime end = new DateTime(0, DateTimeZones.UTC);
//        Interval interval = new Interval(start, end);
//        ScheduleQuery query = ScheduleQuery.single(METABROADCAST,start, end, QueryContext.standard(mock(HttpServletRequest.class)), Id.valueOf(channel.getId()));
//
//        ChannelSchedule channelSchedule = new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of());
//
//        when(channelResolver.fromId(channel.getId()))
//            .thenReturn(Maybe.just(channel));
//        when(scheduleResolver.resolve(argThat(hasItems(channel)), eq(interval), eq(query.getSource())))
//                .thenReturn(Futures.immediateFuture(new Schedule(ImmutableList.of(channelSchedule), interval)));
//
//        QueryResult<ChannelSchedule> result = executor.execute(query);
//
//        assertThat(result.getOnlyResource(), is(channelSchedule));
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
        ScheduleQuery query = ScheduleQuery.multi(METABROADCAST, start, end, QueryContext.standard(mock(HttpServletRequest.class)), cids);

        ChannelSchedule cs1 = new ChannelSchedule(channelOne, interval, ImmutableList.<ItemAndBroadcast>of());
        ChannelSchedule cs2 = new ChannelSchedule(channelTwo, interval, ImmutableList.<ItemAndBroadcast>of());

//        when(channelResolver.forIds(Lists.transform(cids, Id.toLongValue())))
//            .thenReturn(ImmutableList.of(channelOne, channelTwo));
//        when(scheduleResolver.resolve(argThat(hasItems(channelOne, channelTwo)), eq(interval), eq(query.getSource())))
//                .thenReturn(Futures.immediateFuture(new Schedule(ImmutableList.of(cs1, cs2), interval)));
//
        QueryResult<ChannelSchedule> result = executor.execute(query);

//        assertThat(result.getResources().toList(), is(ImmutableList.of(cs1, cs2)));
    }
    
    
    @Test
    public void testThrowsExceptionIfChannelIsMissing() {
        
//        when(channelResolver.fromId(any(Long.class)))
//            .thenReturn(Maybe.<Channel>nothing());

        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);
        Interval interval = new Interval(start, end);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, start, end, QueryContext.standard(mock(HttpServletRequest.class)), Id.valueOf(1));
        
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
        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);
        Interval interval = new Interval(start, end);
        List<SourceReadEntry> reads = ImmutableList.copyOf(Iterables.transform(Publisher.all(), new Function<Publisher, SourceReadEntry>() {
           @Override
            public SourceReadEntry apply(@Nullable Publisher input) {
                return new SourceReadEntry(input, SourceStatus.AVAILABLE_ENABLED);
            }}));
        
        ApplicationSources appSources = ApplicationSources.defaults().copy()
                .withPrecedence(true)
                .withReadableSources(reads)
                .build();
        QueryContext context = new QueryContext(appSources, ActiveAnnotations.standard(), mock(HttpServletRequest.class));
        
        Id itemId = Id.valueOf(1);
        ChannelSchedule channelSchedule = new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of(
            new ItemAndBroadcast(
                new Item(itemId, METABROADCAST), 
                new Broadcast(channel, interval)
            )
        ));

        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, start, end, context, channel.getId());

        Item equivalentItem = new Item(itemId, METABROADCAST);
//        when(channelResolver.fromId(channel.getId()))
//            .thenReturn(Maybe.just(channel));
//        when(scheduleResolver.resolve(argThat(hasItems(channel)), eq(interval), eq(query.getSource())))
//            .thenReturn(Futures.immediateFuture(new Schedule(ImmutableList.of(channelSchedule), interval)));
//        when(equivalentContentResolver.resolveIds(ImmutableSet.of(itemId), appSources, ActiveAnnotations.standard().all()))
//            .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>builder().putEquivalents(itemId, ImmutableList.of(equivalentItem)).build()));
        
        QueryResult<ChannelSchedule> result = executor.execute(query);
        
        assertThat(result.getOnlyResource().getEntries().get(0).getItem(), sameInstance(equivalentItem));
        verify(equivalentContentResolver).resolveIds(ImmutableSet.of(itemId), appSources, ActiveAnnotations.standard().all());
        
    }
    
}
