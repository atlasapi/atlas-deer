package org.atlasapi.query.v4.channel;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.BooleanAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.operator.EqualsOperator;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.criteria.operator.StringOperator;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.bouncycastle.util.Iterable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChannelQueryExecutorTest {

    @Mock
    private ChannelResolver channelResolver;

    @Mock
    private ChannelGroupResolver channelGroupResolver;

    @InjectMocks
    private ChannelQueryExecutor objectUnderTest;

    @Test
    public void testExecuteSingle() throws Exception {
        Id channelId = Id.valueOf(1L);
        Channel result = mock(Channel.class);
        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannel> channelQuery = mock(Query.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);
        when(request.getParameter("annotations")).thenReturn("banana");
        when(context.getRequest()).thenReturn(request);
        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(configuration.isPrecedenceEnabled()).thenReturn(false);
        when(application.getConfiguration()).thenReturn(configuration);
        when(context.getApplication()).thenReturn(application);
        when(channelQuery.isListQuery()).thenReturn(false);
        when(channelQuery.getOnlyId()).thenReturn(channelId);
        when(channelQuery.getContext()).thenReturn(context);
        when(channelResolver.resolveIds(
                (Iterable<Id>) argThat(containsInAnyOrder(channelId)), Matchers.anyBoolean())
        )
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result))
                        )
                );

        QueryResult<ResolvedChannel> queryResult = objectUnderTest.execute(channelQuery);

        assertThat(queryResult.getOnlyResource().getChannel(), is(result));

    }

    @Test
    public void testExecuteMulti() throws Exception {
        Channel result = mock(Channel.class);
        Channel result2 = mock(Channel.class);
        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannel> channelQuery = mock(Query.class);
        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);
        Selection selection = Selection.ALL;
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("annotations")).thenReturn("banana");
        when(context.getRequest()).thenReturn(request);

        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(configuration.isPrecedenceEnabled()).thenReturn(false);
        when(application.getConfiguration()).thenReturn(configuration);
        when(context.getApplication()).thenReturn(application);

        when(context.getSelection()).thenReturn(Optional.of(selection));

        when(channelQuery.isListQuery()).thenReturn(true);
        when(channelQuery.getContext()).thenReturn(context);
        when(channelQuery.getOperands()).thenReturn(Sets.newHashSet());
        when(channelResolver.resolveChannels(any(ChannelQuery.class)))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result, result2))
                        )
                );

        QueryResult<ResolvedChannel> queryResult = objectUnderTest.execute(channelQuery);

        ImmutableList<Channel> channels = queryResult.getResources().toList().stream()
                .map(ResolvedChannel::getChannel)
                .collect(MoreCollectors.toImmutableList());

        assertThat(channels, containsInAnyOrder(result, result2));
    }

    @Test
    public void testSingleChannelIsFullyResolvedByAnnotations() throws Exception {
        Channel result = mock(Channel.class);
        Channel parent = mock(Channel.class);
        ChannelRef parentRef = mock(ChannelRef.class);

        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannel> channelQuery = mock(Query.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);

        Id channelId = Id.valueOf(1L);
        Id parentId = Id.valueOf(2L);

        when(request.getParameter("annotations")).thenReturn("parent");
        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(configuration.isPrecedenceEnabled()).thenReturn(false);
        when(application.getConfiguration()).thenReturn(configuration);
        when(context.getRequest()).thenReturn(request);
        when(context.getApplication()).thenReturn(application);

        when(parentRef.getId()).thenReturn(parentId);
        when(result.getParent()).thenReturn(parentRef);

        when(channelQuery.isListQuery()).thenReturn(false);
        when(channelQuery.getOnlyId()).thenReturn(channelId);
        when(channelQuery.getContext()).thenReturn(context);

        when(channelResolver.resolveIds(
                (Iterable<Id>) argThat(containsInAnyOrder(channelId)), Matchers.anyBoolean())
        )
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result))
                        )
                );

        when(channelResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(parentId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(parent))
                        )
                );

        QueryResult<ResolvedChannel> queryResult = objectUnderTest.execute(channelQuery);

        assertThat(queryResult.getOnlyResource().getChannel(), is(result));
        assert (queryResult.getOnlyResource().getParentChannel().isPresent());
        assertThat(queryResult.getOnlyResource().getChannelGroupSummaries(), is(java.util.Optional.empty()));
        assertThat(queryResult.getOnlyResource().getChannelVariations(), is(java.util.Optional.empty()));
        assertThat(queryResult.getOnlyResource().getParentChannel().get(), is(parent));
    }

    @Test
    public void testMultipleChannelsAreFullyResolvedByAnnotations() throws Exception {
        Channel result = mock(Channel.class);
        Channel result2 = mock(Channel.class);
        ChannelGroupMembership channelGroupMembership = mock(ChannelGroupMembership.class);
        ChannelGroup<?> channelGroup = mock(ChannelGroup.class);
        ChannelGroupRef channelGroupRef = mock(ChannelGroupRef.class);
        ChannelGroupSummary channelGroupSummary = mock(ChannelGroupSummary.class);
        ChannelRef channelRef = mock(ChannelRef.class);

        Id channelGroupId = Id.valueOf(5L);
        Id variationId = Id.valueOf(10L);

        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannel> channelQuery = mock(Query.class);

        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);
        Selection selection = Selection.ALL;
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getParameter("annotations")).thenReturn("variations,channel_groups_summary");
        when(context.getRequest()).thenReturn(request);

        when(application.getConfiguration()).thenReturn(configuration);
        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(configuration.isPrecedenceEnabled()).thenReturn(false);
        when(context.getApplication()).thenReturn(application);

        when(context.getSelection()).thenReturn(Optional.of(selection));

        when(channelGroupRef.getId()).thenReturn(channelGroupId);
        when(channelGroupMembership.getChannelGroup()).thenReturn(channelGroupRef);

        when(channelRef.getId()).thenReturn(variationId);
        when(result.getVariations()).thenReturn(ImmutableSet.of(channelRef));
        when(result2.getVariations()).thenReturn(ImmutableSet.of(channelRef));

        when(channelGroup.toSummary()).thenReturn(channelGroupSummary);

        when(result.getChannelGroups())
                .thenReturn(ImmutableSet.of(channelGroupMembership));
        when(result2.getChannelGroups())
                .thenReturn(ImmutableSet.of(channelGroupMembership));

        when(channelQuery.isListQuery()).thenReturn(true);
        when(channelQuery.getContext()).thenReturn(context);
        when(channelQuery.getOperands()).thenReturn(Sets.newHashSet());

        when(channelResolver.resolveChannels(any(ChannelQuery.class)))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result, result2))
                        )
                );

        when(channelGroupResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(
                channelGroupId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableList.of(channelGroup))
                        )
                );

        when(channelResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(variationId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableList.of(result))
                        )
                );

        QueryResult<ResolvedChannel> queryResult = objectUnderTest.execute(channelQuery);

        ImmutableList<Channel> channels = queryResult.getResources().toList().stream()
                .map(ResolvedChannel::getChannel)
                .collect(MoreCollectors.toImmutableList());

        assertThat(channels, containsInAnyOrder(result, result2));

        ImmutableList<ResolvedChannel> resolvedChannels = queryResult.getResources().toList();

        for (ResolvedChannel resolvedChannel : resolvedChannels) {
            assertThat(resolvedChannel.getParentChannel(), is(java.util.Optional.empty()));
            assert (resolvedChannel.getChannelGroupSummaries().isPresent());
            assert (resolvedChannel.getChannelVariations().isPresent());

            assert (resolvedChannel.getChannelGroupSummaries().get().contains(channelGroupSummary));
        }
    }

    @Test
    public void testMultipleChannelsAreFullyResolvedOnlyByAliasAnnotations() throws Exception {
        Channel channel1 = mock(Channel.class);
        Channel channel2 = mock(Channel.class);

        StringOperator op = Operators.BEGINNING;
        AttributeQuery namespaceAttribute = new StringAttributeQuery(
                Attributes.ALIASES_NAMESPACE,
                op,
                ImmutableList.of("namespace")
        );
        AttributeQuery valueAttribute = new StringAttributeQuery(
                Attributes.ALIASES_VALUE,
                op,
                ImmutableList.of("value")
        );

        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannel> query = mock(Query.class);

        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);

        Selection selection = Selection.ALL;

        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getParameter("annotations")).thenReturn("aliases.namespace, aliases.value");
        when(context.getRequest()).thenReturn(request);

        when(application.getConfiguration()).thenReturn(configuration);
        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(configuration.isPrecedenceEnabled()).thenReturn(false);
        when(context.getApplication()).thenReturn(application);

        when(context.getSelection()).thenReturn(Optional.of(selection));

        when(query.isListQuery()).thenReturn(true);
        when(query.getContext()).thenReturn(context);
        when(query.getOperands()).thenReturn(ImmutableList.<AttributeQuery<Object>>of(
                namespaceAttribute,
                valueAttribute
        ));

        when(channelResolver.resolveChannelsWithAliases(any(ChannelQuery.class)))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(channel1, channel2))
                        )
                );

        QueryResult<ResolvedChannel> queryResult = objectUnderTest.execute(query);

        verify(channelResolver).resolveChannelsWithAliases(any(ChannelQuery.class));

        ImmutableList<Channel> channels = queryResult.getResources()
                .toList()
                .stream()
                .map(ResolvedChannel::getChannel)
                .collect(MoreCollectors.toImmutableList());

        assertThat(channels, containsInAnyOrder(channel1, channel2));
    }

    @Test
    public void testMultipleChannelsAreFullyResolvedByAliasAndOtherAnnotations() throws Exception {
        Channel channel1 = mock(Channel.class);
        Channel channel2 = mock(Channel.class);

        StringOperator stringOperator = Operators.BEGINNING;
        EqualsOperator booleanOperator = Operators.EQUALS;
        AttributeQuery namespaceAttribute = new StringAttributeQuery(
                Attributes.ALIASES_NAMESPACE,
                stringOperator,
                ImmutableList.of("namespace")
        );
        AttributeQuery valueAttribute = new StringAttributeQuery(
                Attributes.ALIASES_VALUE,
                stringOperator,
                ImmutableList.of("value")
        );
        AttributeQuery advertisedOnAttribute = new BooleanAttributeQuery(
                Attributes.ADVERTISED_ON,
                booleanOperator,
                ImmutableList.of(true)
        );

        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannel> query = mock(Query.class);

        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);

        Selection selection = Selection.ALL;

        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getParameter("annotations")).thenReturn(
                "aliases.namespace, aliases.value, advertised"
        );
        when(context.getRequest()).thenReturn(request);

        when(application.getConfiguration()).thenReturn(configuration);
        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(configuration.isPrecedenceEnabled()).thenReturn(false);
        when(context.getApplication()).thenReturn(application);

        when(context.getSelection()).thenReturn(Optional.of(selection));

        when(query.isListQuery()).thenReturn(true);
        when(query.getContext()).thenReturn(context);
        when(query.getOperands()).thenReturn(
                ImmutableList.<AttributeQuery<Object>>of(
                        namespaceAttribute,
                        valueAttribute,
                        advertisedOnAttribute
                )
        );

        when(channelResolver.resolveChannels(any(ChannelQuery.class)))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(channel1, channel2))
                        )
                );

        QueryResult<ResolvedChannel> queryResult = objectUnderTest.execute(query);

        verify(channelResolver).resolveChannels(any(ChannelQuery.class));

        ImmutableList<Channel> channels = queryResult.getResources()
                .toList()
                .stream()
                .map(ResolvedChannel::getChannel)
                .collect(MoreCollectors.toImmutableList());

        assertThat(channels, containsInAnyOrder(channel1, channel2));
    }

}
