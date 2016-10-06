package org.atlasapi.query.v4.channelgroup;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.metabroadcast.common.query.Selection;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
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
        ApplicationSources applicationSources = mock(ApplicationSources.class);
        Selection selection = Selection.ALL;
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(applicationSources.isReadEnabled(any(Publisher.class))).thenReturn(true);

        when(context.getApplicationSources()).thenReturn(applicationSources);
        when(context.getSelection()).thenReturn(Optional.of(selection));
        when(context.getRequest()).thenReturn(request);

        when(request.getParameter("annotations")).thenReturn("regions");

        when(channelQuery.isListQuery()).thenReturn(true);
        when(channelQuery.getContext()).thenReturn(context);

        AttributeQuery attributeQuery = mock(AttributeQuery.class);
        when(attributeQuery.getAttributeName()).thenReturn("type");
        when(attributeQuery.getValue()).thenReturn(ImmutableList.of("platform"));

        Attribute attribute = mock(Attribute.class);
        when(attributeQuery.getAttribute()).thenReturn(attribute);
        when(attribute.getPath()).thenReturn(ImmutableList.of("path"));

        AttributeQuerySet attributeQueries = new AttributeQuerySet(Sets.<AttributeQuery<Object>>newHashSet(
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
    public void testSingleContentIsFullyResolvedByAnnotation() throws Exception{

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

        assertThat(queryResult.getOnlyResource().getPlatformChannelGroup(), is(Optional.absent()));
    }
}