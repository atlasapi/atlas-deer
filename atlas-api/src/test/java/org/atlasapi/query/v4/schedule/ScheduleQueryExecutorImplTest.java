package org.atlasapi.query.v4.schedule;

import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleResolver;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
public class ScheduleQueryExecutorImplTest {

    @Mock private MergingEquivalentsResolver<Content> equivalentContentResolver;
    @Mock private ChannelResolver channelResolver;
    @Mock private ScheduleResolver scheduleResolver;
    @Mock private Application application = mock(Application.class);
    @Mock private HttpServletRequest request = mock(HttpServletRequest.class);

    private ScheduleQueryExecutorImpl executor;
    private QueryContext queryContext;

    @Before
    public void setup() {
        executor = new ScheduleQueryExecutorImpl(
                channelResolver,
                scheduleResolver,
                equivalentContentResolver
        );

        when(application.getConfiguration())
                .thenReturn(
                        getConfigWithNoPrecedence(Publisher.all().stream()
                                .filter(Publisher::enabledWithNoApiKey)
                                .collect(Collectors.toList())
                )
        );
        queryContext = QueryContext.create(
                application,
                ActiveAnnotations.standard(),
                request
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

        ScheduleQuery query = ScheduleQuery.builder()
                .withSource(METABROADCAST)
                .withStart(start)
                .withEnd(end)
                .withContext(queryContext)
                .withId(channel.getId())
                .build();

        ChannelSchedule channelSchedule = new ChannelSchedule(
                channel,
                interval,
                ImmutableList.of()
        );

        when(channelResolver.resolveIds(argThat(org.hamcrest.Matchers.iterableWithSize(1))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(channel))));
        when(scheduleResolver.resolve(
                argThat(hasItems(channel)),
                eq(interval),
                eq(query.getSource())
        ))
                .thenReturn(Futures.immediateFuture(new Schedule(
                        ImmutableList.of(channelSchedule),
                        interval
                )));

        QueryResult<ChannelSchedule> result = executor.execute(query);

        assertThat(result.getOnlyResource(), is(channelSchedule));
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

        ScheduleQuery query = ScheduleQuery.builder()
                .withSource(METABROADCAST)
                .withStart(start)
                .withEnd(end)
                .withContext(queryContext)
                .withIds(cids)
                .build();

        ChannelSchedule cs1 = new ChannelSchedule(
                channelOne,
                interval,
                ImmutableList.of()
        );
        ChannelSchedule cs2 = new ChannelSchedule(
                channelTwo,
                interval,
                ImmutableList.of()
        );

        when(channelResolver.resolveIds(argThat(org.hamcrest.Matchers.iterableWithSize(2))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(
                        channelOne,
                        channelTwo
                ))));
        when(scheduleResolver.resolve(
                argThat(hasItems(channelOne, channelTwo)),
                eq(interval),
                eq(query.getSource())
        ))
                .thenReturn(Futures.immediateFuture(new Schedule(
                        ImmutableList.of(cs1, cs2),
                        interval
                )));

        QueryResult<ChannelSchedule> result = executor.execute(query);

        assertThat(result.getResources().toList(), is(ImmutableList.of(cs1, cs2)));
    }

    @Test
    public void testThrowsExceptionIfChannelIsMissing() {

        when(channelResolver.resolveIds(any(Iterable.class)))
                .thenReturn(Futures.immediateFuture(Resolved.empty()));

        DateTime start = new DateTime(0, DateTimeZones.UTC);
        DateTime end = new DateTime(0, DateTimeZones.UTC);

        ScheduleQuery query = ScheduleQuery.builder()
                .withSource(METABROADCAST)
                .withStart(start)
                .withEnd(end)
                .withContext(queryContext)
                .withId(Id.valueOf(1))
                .build();

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

        Application application = mock(Application.class);
        when(application.getConfiguration())
                .thenReturn(getConfigWithPrecedence(Publisher.all().asList()));
        QueryContext context = QueryContext.create(
                application,
                ActiveAnnotations.standard(),
                mock(HttpServletRequest.class)
        );

        Id itemId = Id.valueOf(1);
        ChannelSchedule channelSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(
                        new Item(itemId, METABROADCAST),
                        new Broadcast(channel, interval)
                )
        ));

        ScheduleQuery query = ScheduleQuery.builder()
                .withSource(METABROADCAST)
                .withStart(start)
                .withEnd(end)
                .withContext(context)
                .withId(channel.getId())
                .build();

        Item equivalentItem = new Item(itemId, METABROADCAST);
        when(channelResolver.resolveIds(argThat(org.hamcrest.Matchers.iterableWithSize(1))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(channel))));
        when(scheduleResolver.resolve(
                argThat(hasItems(channel)),
                eq(interval),
                eq(query.getSource())
        ))
                .thenReturn(Futures.immediateFuture(new Schedule(
                        ImmutableList.of(channelSchedule),
                        interval
                )));
        when(equivalentContentResolver.resolveIds(
                ImmutableSet.of(itemId),
                application,
                ActiveAnnotations.standard().all(),
                Set<AttributeQuery<?>>.create(ImmutableSet.of())
        ))
                .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>builder().putEquivalents(
                        itemId,
                        ImmutableList.of(equivalentItem)
                ).build()));

        QueryResult<ChannelSchedule> result = executor.execute(query);

        assertThat(
                result.getOnlyResource().getEntries().get(0).getItem(),
                sameInstance(equivalentItem)
        );
        verify(equivalentContentResolver).resolveIds(
                ImmutableSet.of(itemId),
                application,
                ActiveAnnotations.standard().all(),
                Set<AttributeQuery<?>>.create(ImmutableSet.of())
        );

    }

    private ApplicationConfiguration getConfigWithNoPrecedence(List<Publisher> publishers) {
        return ApplicationConfiguration.builder()
                .withNoPrecedence(publishers)
                .withEnabledWriteSources(ImmutableList.of())
                .build();
    }

    private ApplicationConfiguration getConfigWithPrecedence(List<Publisher> publishers) {
        return ApplicationConfiguration.builder()
                .withPrecedence(publishers)
                .withEnabledWriteSources(ImmutableList.of())
                .build();
    }

}
