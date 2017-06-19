package org.atlasapi.query.v4.channelgroup;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.query.Selection;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.joda.time.LocalDate;
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
public class ChannelGroupQueryExecutorTest {

    @Mock
    private ChannelGroupResolver channelGroupResolver;

    @Mock
    private ChannelResolver channelResolver;

    @InjectMocks
    private ChannelGroupQueryExecutor objectUnderTest;

    @Test
    public void testExecuteSingle() throws Exception {
        Id channelGroupId = Id.valueOf(1L);
        ChannelGroup result = mock(ChannelGroup.class);
        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannelGroup> channelQuery = mock(Query.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("annotations")).thenReturn("banana");
        when(context.getRequest()).thenReturn(request);
        when(channelQuery.isListQuery()).thenReturn(false);
        when(channelQuery.getOnlyId()).thenReturn(channelGroupId);
        when(channelQuery.getContext()).thenReturn(context);
        when(channelGroupResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(
                channelGroupId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result))
                        )
                );

        QueryResult<ResolvedChannelGroup> queryResult = objectUnderTest.execute(channelQuery);

        assertThat(queryResult.getOnlyResource().getChannelGroup(), is(result));

    }

    @Test
    public void testExecuteMulti() throws Exception {
        ChannelGroup result = mock(ChannelGroup.class);
        ChannelGroup result2 = mock(ChannelGroup.class);
        ChannelGroup result3 = mock(ChannelGroup.class);

        when(result.getType()).thenReturn("platform");
        when(result2.getType()).thenReturn("region");
        when(result3.getType()).thenReturn("platform");

        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannelGroup> channelQuery = mock(Query.class);
        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);
        Selection selection = Selection.ALL;
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(application.getConfiguration()).thenReturn(configuration);

        when(context.getApplication()).thenReturn(application);
        when(context.getSelection()).thenReturn(Optional.of(selection));
        when(context.getRequest()).thenReturn(request);

        when(request.getParameter("annotations")).thenReturn("banana");

        when(channelQuery.isListQuery()).thenReturn(true);
        when(channelQuery.getContext()).thenReturn(context);

        AttributeQuery attributeQuery = mock(AttributeQuery.class);
        when(attributeQuery.getAttributeName()).thenReturn("type");
        when(attributeQuery.getValue()).thenReturn(ImmutableList.of("platform"));

        Attribute attribute = mock(Attribute.class);
        when(attributeQuery.getAttribute()).thenReturn(attribute);
        when(attribute.getPath()).thenReturn(ImmutableList.of("path"));

        AttributeQuerySet attributeQueries = AttributeQuerySet.create(Sets.<AttributeQuery<Object>>newHashSet(
                attributeQuery));

        when(channelQuery.getOperands()).thenReturn(attributeQueries);

        when(channelGroupResolver.allChannels())
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result, result2, result3))
                        )
                );

        QueryResult<ResolvedChannelGroup> queryResult = objectUnderTest.execute(channelQuery);


        assertThat(queryResult.getResources()
                .toList()
                .stream()
                .map(ResolvedChannelGroup::getChannelGroup)
                .collect(Collectors.toList()),
                containsInAnyOrder(result, result3));
    }

    @Test
    public void testSingleChannelGroupIsFullyResolvedByAnnotation() throws Exception {

        Platform testChannelGroup = mock(Platform.class);
        ChannelGroup<?> testRegionChannelGroup = mock(ChannelGroup.class);

        QueryContext context = mock(QueryContext.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        Query<ResolvedChannelGroup> channelQuery = mock(Query.class);
        ChannelGroupRef regionChannelGroupRef = mock(ChannelGroupRef.class);
        Set<ChannelGroupRef> regionChannelGroupRefSet = new HashSet<>();
        Id channelGroupId = Id.valueOf(1L);
        Id regionChannelGroupId = Id.valueOf(2L);

        when(request.getParameter("annotations")).thenReturn("regions");
        when(context.getRequest()).thenReturn(request);

        when(channelQuery.getContext()).thenReturn(context);
        when(channelQuery.getOnlyId()).thenReturn(channelGroupId);
        when(channelQuery.isListQuery()).thenReturn(false);

        when(testChannelGroup.getType()).thenReturn("platform");
        when(regionChannelGroupRef.getId()).thenReturn(regionChannelGroupId);
        regionChannelGroupRefSet.add(regionChannelGroupRef);
        when(testChannelGroup.getRegions()).thenReturn(regionChannelGroupRefSet);

        when(channelGroupResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(
                channelGroupId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(testChannelGroup))
                        )
                );
        when(channelGroupResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(
                regionChannelGroupId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(testRegionChannelGroup))
                        )
                );

        QueryResult<ResolvedChannelGroup> queryResult = objectUnderTest.execute(channelQuery);

        assert(queryResult.getOnlyResource().getRegionChannelGroups().isPresent());
        assertThat(queryResult.getOnlyResource().getRegionChannelGroups().get().iterator().next(),
                is(testRegionChannelGroup));

        assertThat(queryResult.getOnlyResource().getPlatformChannelGroup(), is(java.util.Optional.empty()));
        assertThat(queryResult.getOnlyResource().getChannels(), is(java.util.Optional.empty()));
    }

    @Test
    public void testMultipleChannelGroupsAreFullyResolvedByAnnotations() throws Exception {
        Platform testChannelGroup = mock(Platform.class);
        ChannelGroup testChannelGroup2 = mock(ChannelGroup.class);
        Platform testChannelGroup3 = mock(Platform.class);
        Channel channel = mock(Channel.class);
        Channel channel2 = mock(Channel.class);
        ChannelGroup regionChannelGroup = mock(ChannelGroup.class);
        ChannelNumbering channelNumbering = mock(ChannelNumbering.class);
        ChannelNumbering channelNumbering2 = mock(ChannelNumbering.class);

        Id channelGroupId = Id.valueOf(1L);
        Id channelId = Id.valueOf(10L);
        Id channelId2 = Id.valueOf(11L);

        ChannelGroupRef testChannelGroupRef = mock(ChannelGroupRef.class);

        Set<ChannelGroupRef> testChannelGroupRefSet = new HashSet<>();
        when(testChannelGroupRef.getId()).thenReturn(channelGroupId);
        testChannelGroupRefSet.add(testChannelGroupRef);
        when(testChannelGroup.getRegions()).thenReturn(testChannelGroupRefSet);
        when(testChannelGroup3.getRegions()).thenReturn(testChannelGroupRefSet);


        when(channel.getId()).thenReturn(channelId);
        when(channel2.getId()).thenReturn(channelId2);

        ChannelRef channelRef = mock(ChannelRef.class);
        ChannelRef channelRef2 = mock(ChannelRef.class);

        when(channelRef.getId()).thenReturn(channelId);
        when(channelRef2.getId()).thenReturn(channelId2);

        when(channelNumbering.getChannel()).thenReturn(channelRef);
        when(channelNumbering2.getChannel()).thenReturn(channelRef2);
        Iterable<ChannelNumbering> channels = Lists.newArrayList(channelNumbering, channelNumbering2);

        when(testChannelGroup.getChannelsAvailable(any(LocalDate.class)))
            .thenReturn(channels);
        when(testChannelGroup3.getChannelsAvailable(any(LocalDate.class)))
            .thenReturn(channels);

        when(testChannelGroup.getType()).thenReturn("platform");
        when(testChannelGroup2.getType()).thenReturn("region");
        when(testChannelGroup3.getType()).thenReturn("platform");

        QueryContext context = mock(QueryContext.class);
        Query<ResolvedChannelGroup> channelQuery = mock(Query.class);
        Application application = mock(Application.class);
        ApplicationConfiguration configuration = mock(ApplicationConfiguration.class);
        Selection selection = Selection.ALL;
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(configuration.isReadEnabled(any(Publisher.class))).thenReturn(true);
        when(application.getConfiguration()).thenReturn(configuration);

        when(request.getParameter("annotations")).thenReturn("regions,channels");

        when(context.getApplication()).thenReturn(application);
        when(context.getSelection()).thenReturn(Optional.of(selection));
        when(context.getRequest()).thenReturn(request);

        when(channelQuery.isListQuery()).thenReturn(true);
        when(channelQuery.getContext()).thenReturn(context);

        AttributeQuery attributeQuery = mock(AttributeQuery.class);
        when(attributeQuery.getAttributeName()).thenReturn("type");
        when(attributeQuery.getValue()).thenReturn(ImmutableList.of("platform"));

        Attribute attribute = mock(Attribute.class);
        when(attributeQuery.getAttribute()).thenReturn(attribute);
        when(attribute.getPath()).thenReturn(ImmutableList.of("path"));

        AttributeQuerySet attributeQueries = AttributeQuerySet.create(Sets.<AttributeQuery<Object>>newHashSet(
                attributeQuery));

        when(channelQuery.getOperands()).thenReturn(attributeQueries);

        when(channelGroupResolver.allChannels())
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(testChannelGroup, testChannelGroup2, testChannelGroup3))
                        )
                );

        when(channelGroupResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(
                channelGroupId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(regionChannelGroup))
                        )
                );

        when(channelResolver.resolveIds(any()))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(channel, channel2))
                        )
                );

        QueryResult<ResolvedChannelGroup> queryResult = objectUnderTest.execute(channelQuery);

        List<ResolvedChannelGroup> resolvedChannelGroups = queryResult.getResources().toList();
        for(ResolvedChannelGroup resolvedChannelGroup : resolvedChannelGroups) {
            assert(resolvedChannelGroup.getChannels().isPresent());

            if(resolvedChannelGroup.getChannelGroup().getType().equals("platform")) {
                assert (resolvedChannelGroup.getRegionChannelGroups().isPresent());
            } else {
                assertThat(resolvedChannelGroup.getRegionChannelGroups(), is(Optional.absent()));
            }

            List<Channel> channelGroupsChannels = StreamSupport.stream(
                    resolvedChannelGroup.getChannels().get().spliterator(), false)
                    .map(ResolvedChannel::getChannel)
                    .collect(Collectors.toList());

            assert(channelGroupsChannels.containsAll(ImmutableList.of(channel, channel2)));
            assertThat(resolvedChannelGroup.getPlatformChannelGroup(), is(java.util.Optional.empty()));
        }

    }

    @Test
    public void testChannelGroupChannelHasFilteredChannelGroups() throws Exception {
        assert(true); // TODO: write me (MBST-17114)
    }
}
