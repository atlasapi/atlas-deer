package org.atlasapi.query.v4.channelgroup;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.promise.Promise;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryExecutor implements QueryExecutor<ResolvedChannelGroup> {

    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelResolver channelResolver;

    public ChannelGroupQueryExecutor(ChannelGroupResolver channelGroupResolver, ChannelResolver channelResolver) {
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.channelResolver = channelResolver;
    }

    @Nonnull
    @Override
    public QueryResult<ResolvedChannelGroup> execute(Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private QueryResult<ResolvedChannelGroup> executeSingleQuery(Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        channelGroupResolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        (Resolved<ChannelGroup<?>> input) -> {
                            if (input.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(new NotFoundException(
                                        query.getOnlyId()));
                            }

                            ResolvedChannelGroup resolvedChannelGroup =
                                    resolveAnnotationData(
                                            query.getContext(),
                                            input.getResources().first().get()
                                    );

                            return QueryResult.singleResult(
                                    resolvedChannelGroup,
                                    query.getContext()
                            );
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );
    }

    private QueryResult<ResolvedChannelGroup> executeListQuery(Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        Iterable<ChannelGroup<?>> channelGroups = Futures.get(
                Futures.transform(
                        channelGroupResolver.allChannels(),
                        (Resolved<ChannelGroup<?>> input) -> {
                            return input.getResources();
                        }
                ),
                1, TimeUnit.MINUTES,
                QueryExecutionException.class
        );

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_TYPE.externalName())) {
                final String channelGroupType = attributeQuery.getValue().get(0).toString();
                channelGroups = StreamSupport.stream(channelGroups.spliterator(), false)
                        .filter(channelGroup -> channelGroupType.equals(channelGroup.getType()))
                        .collect(Collectors.toList());

            }
        }

        ImmutableList<ChannelGroup<?>> filteredChannelGroups = ImmutableList.copyOf(
                StreamSupport.stream(
                    channelGroups.spliterator(),
                    false
                ).filter(input -> query.getContext()
                        .getApplicationSources()
                        .isReadEnabled(input.getSource())
                ).collect(Collectors.toList()));

        ImmutableList<ChannelGroup<?>> selectedChannelGroups =
                query.getContext()
                        .getSelection()
                        .get()
                        .applyTo(filteredChannelGroups);

        ImmutableList<ResolvedChannelGroup> resolvedChannelGroups =
                selectedChannelGroups.stream()
                        .map(channelGroup -> resolveAnnotationData(query.getContext(), channelGroup))
                        .collect(MoreCollectors.toImmutableList());


        return QueryResult.listResult(
                resolvedChannelGroups,
                query.getContext(),
                resolvedChannelGroups.size()
        );
    }

    private ResolvedChannelGroup resolveAnnotationData(
            QueryContext ctxt,
            ChannelGroup<?> channelGroup
    ) {
        ResolvedChannelGroup.Builder resolvedChannelGroupBuilder =
                ResolvedChannelGroup.builder(channelGroup);

        resolvedChannelGroupBuilder.withRegionChannelGroups(
                contextHasAnnotation(ctxt, Annotation.REGIONS) ?
                    resolveRegionChannelGroups(channelGroup) :
                    Optional.absent()
        );

        resolvedChannelGroupBuilder.withPlatformChannelGroup(
                contextHasAnnotation(ctxt, Annotation.PLATFORM) ?
                    resolvePlatformChannelGroup(channelGroup) :
                    Optional.absent()
        );

        resolvedChannelGroupBuilder.withAdvertisedChannels(
                contextHasAnnotation(ctxt, Annotation.ADVERTISED_CHANNELS) ?
                    resolveAdvertisedChannels(channelGroup) :
                    Optional.absent()
        );

        return resolvedChannelGroupBuilder.build();
    }

    private Optional<Iterable<ChannelGroup<?>>> resolveRegionChannelGroups(ChannelGroup<?> entity) {

        if(!(entity instanceof  Platform)) {
            return Optional.absent();
        }

        Platform platform = (Platform) entity;
        Iterable<Id> regionIds = platform.getRegions()
                .stream()
                .map(ChannelGroupRef::getId)
                .collect(MoreCollectors.toImmutableSet());

        return Optional.of(Promise.wrap(channelGroupResolver.resolveIds(regionIds))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES));
    }

    private Optional<ChannelGroup<?>> resolvePlatformChannelGroup(ChannelGroup<?> entity) {
        if(!(entity instanceof Region)) {
            return Optional.absent();
        }

        Id platformId = ((Region) entity).getPlatform().getId();

        return Promise.wrap(channelGroupResolver.resolveIds(ImmutableSet.of(platformId)))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES)
                .first();
    }

    private Optional<Iterable<Channel>> resolveAdvertisedChannels(ChannelGroup<?> entity) {

        final ImmutableMultimap.Builder<Id, ChannelGroupMembership> builder = ImmutableMultimap.builder();

        Iterable<? extends ChannelGroupMembership> availableChannels = entity.getChannelsAvailable(
                LocalDate.now());

        List<Id> orderedIds = StreamSupport.stream(availableChannels.spliterator(), false)
                //TODO fix channel appearing twice in ordering blowing this thing up
                .map(cm -> cm.getChannel().getId())
                .distinct()
                .collect(Collectors.toList());
        Ordering<Id> idOrdering = Ordering.explicit(orderedIds);

        for (ChannelGroupMembership channelGroupMembership : availableChannels) {
            builder.put(channelGroupMembership.getChannel().getId(), channelGroupMembership);
        }

        ImmutableMultimap<Id, ChannelGroupMembership> channelGroupMemberships = builder.build();

        Iterable<Channel> resolvedChannels = Promise.wrap(
                channelResolver.resolveIds(channelGroupMemberships.keySet()))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES);

        Iterable<Channel> sortedChannels = StreamSupport.stream(resolvedChannels.spliterator(), false)
                .sorted((o1, o2) -> idOrdering.compare(o1.getId(), o2.getId()))
                .collect(Collectors.toList());

        return Optional.of(sortedChannels);
    }

    private boolean contextHasAnnotation(QueryContext ctxt, Annotation annotation) {

        return (!Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
            &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        ).contains(annotation.toKey()));
    }
}
