package org.atlasapi.query.v4.channel;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChannelQueryExecutorTest {

    @Mock
    private ChannelResolver channelResolver;


    @InjectMocks
    private ChannelQueryExecutor objectUnderTest;
    @Test
    public void testExecuteSingle() throws Exception {
        Id channelId = Id.valueOf(1L);
        Channel result = mock(Channel.class);
        QueryContext context = mock(QueryContext.class);
        Query<Channel> channelQuery = mock(Query.class);
        when(channelQuery.isListQuery()).thenReturn(false);
        when(channelQuery.getOnlyId()).thenReturn(channelId);
        when(channelQuery.getContext()).thenReturn(context);
        when(channelResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(channelId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result))
                        )
                );


        QueryResult<Channel> queryResult = objectUnderTest.execute(channelQuery);


        assertThat(queryResult.getOnlyResource(), is(result));


    }


    @Test
    public void testExecuteMulti() throws Exception {
        Channel result = mock(Channel.class);
        Channel result2 = mock(Channel.class);
        QueryContext context = mock(QueryContext.class);
        Query<Channel> channelQuery = mock(Query.class);
        ApplicationSources applicationSources = mock(ApplicationSources.class);
        Selection selection = Selection.ALL;

        when(applicationSources.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(context.getApplicationSources()).thenReturn(applicationSources);

        when(context.getSelection()).thenReturn(Optional.of(selection));

        when(channelQuery.isListQuery()).thenReturn(true);
        when(channelQuery.getContext()).thenReturn(context);
        when(channelQuery.getOperands()).thenReturn(
                new AttributeQuerySet(Sets.<AttributeQuery<Object>>newHashSet())
        );
        when(channelResolver.resolveChannels(any(ChannelQuery.class)))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result, result2))
                        )
                );


        QueryResult<Channel> queryResult = objectUnderTest.execute(channelQuery);


        assertThat(queryResult.getResources(), containsInAnyOrder(result, result2));
    }

}