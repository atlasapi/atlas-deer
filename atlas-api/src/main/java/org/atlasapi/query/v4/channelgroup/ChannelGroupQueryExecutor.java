package org.atlasapi.query.v4.channelgroup;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.promise.Promise;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.NumberedChannelGroup;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelGroupQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.exceptions.UncheckedQueryExecutionException;
import org.joda.time.LocalDate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryExecutor implements QueryExecutor<ResolvedChannelGroup> {

    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelResolver channelResolver;

    public ChannelGroupQueryExecutor(
            ChannelGroupResolver channelGroupResolver,
            ChannelResolver channelResolver
    ) {
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
                        channelGroupResolver.resolveIds(
                                ImmutableSet.of(query.getOnlyId()),
                                Boolean.parseBoolean(
                                        query.getContext().getRequest().getParameter(
                                                Attributes.REFRESH_CACHE.externalName()
                                        )
                                )
                        ),
                        (Resolved<ChannelGroup<?>> resolved) -> {
                            if (resolved.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(
                                        new NotFoundException(query.getOnlyId())
                                );
                            }

                            ChannelGroup<?> channelGroup = resolved.getResources()
                                    .first()
                                    .get();

                            NumberedChannelGroup.ChannelOrdering channelOrdering =
                                    NumberedChannelGroup.ChannelOrdering.CHANNEL_NUMBER;
                            Set<Id> dttIds = null;
                            Set<Id> ipIds = null;

                            for (AttributeQuery<?> attributeQuery : query.getOperands()) {
                                String attributeName = attributeQuery.getAttributeName();
                                List<?> attributeValue = attributeQuery.getValue();

                                if (attributeName.equals(Attributes.CHANNEL_GROUP_DTT_CHANNELS.externalName())) {
                                    Set<Id> newDttIds = ImmutableSet.copyOf((List<Id>) attributeValue);
                                    dttIds = dttIds == null ? newDttIds : Sets.union(dttIds, newDttIds);
                                } else if (attributeName.equals(Attributes.CHANNEL_GROUP_IP_CHANNELS.externalName())) {
                                    Set<Id> newIpIds = ImmutableSet.copyOf((List<Id>) attributeValue);
                                    ipIds = ipIds == null ? newIpIds : Sets.union(ipIds, newIpIds);
                                } else if (attributeName.equals(Attributes.CHANNEL_ORDERING.externalName())) {
                                    String orderingName = attributeValue.get(0).toString();
                                    channelOrdering = NumberedChannelGroup.ChannelOrdering.forName(orderingName);
                                    if (channelOrdering == null) {
                                        throw new IllegalArgumentException(
                                                "Invalid channel ordering: " + orderingName + " (possible values: " +
                                                        NumberedChannelGroup.ChannelOrdering.names() + ")"
                                        );
                                    }
                                }
                            }

                            ChannelGroup<?> channelFilteredChannelGroup = Iterables.getOnlyElement(
                                    getChannelFilteredChannelGroups(
                                            ImmutableList.of(channelGroup),
                                            dttIds,
                                            ipIds
                                    )
                            );

                            ResolvedChannelGroup resolvedChannelGroup = resolveAnnotationData(
                                    query.getContext(),
                                    channelFilteredChannelGroup,
                                    channelOrdering
                            );

                            return QueryResult.singleResult(
                                    resolvedChannelGroup,
                                    query.getContext()
                            );
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );
    }

    private Iterable<ChannelGroup<?>> getChannelFilteredChannelGroups(
            Iterable<ChannelGroup<?>> channelGroups,
            @Nullable Set<Id> dttIds,
            @Nullable Set<Id> ipIds
    ) {
        ImmutableList.Builder<ChannelGroup<?>> filteredChannelGroups = ImmutableList.builder();
        for (ChannelGroup<?> channelGroup : channelGroups) {
            boolean changed = false;
            if (channelGroup instanceof NumberedChannelGroup) {
                NumberedChannelGroup numberedChannelGroup = (NumberedChannelGroup) channelGroup;
                boolean filterToDttChannels = dttIds != null && !dttIds.isEmpty() &&
                        dttIds.contains(channelGroup.getId());
                boolean filterToIpChannels = ipIds != null && !ipIds.isEmpty() &&
                        ipIds.contains(channelGroup.getId());
                if (filterToDttChannels || filterToIpChannels) {
                    changed = true;
                    ChannelGroup<?> filteredChannelGroup = filterChannelGroupChannels(
                            numberedChannelGroup,
                            filterToDttChannels,
                            filterToIpChannels
                    );
                    filteredChannelGroups.add(filteredChannelGroup);
                }
            }
            if (!changed) {
                filteredChannelGroups.add(channelGroup);
            }
        }
        return filteredChannelGroups.build();
    }

    private ChannelGroup<?> filterChannelGroupChannels(
            NumberedChannelGroup channelGroup,
            boolean filterToDttChannels,
            boolean filterToIpChannels
    ) {
        Iterable<ChannelNumbering> filteredChannels = channelGroup.getChannels(
                NumberedChannelGroup.ChannelOrdering.SPECIFIED
        )
                .stream()
                .filter(channel -> channel.getChannelNumber().isPresent())
                .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                .filter(channel -> {
                    int channelNumber = Integer.parseInt(channel.getChannelNumber().get());
                    // Is this really what we want? If the same channel group is passed in both query
                    // parameters we will not include any channels. The logic has been preserved during
                    // a refactor of the code but this might just be unintentional.
                    boolean include = true;
                    if (filterToDttChannels) {
                        include = channelNumber <= 300;
                    }
                    if (filterToIpChannels) {
                        include &= channelNumber > 300;
                    }
                    return include;
                })
                .collect(MoreCollectors.toImmutableSet());
        return channelGroup.copyWithChannels(filteredChannels);
    }

    private QueryResult<ResolvedChannelGroup> executeListQuery(
            Query<ResolvedChannelGroup> query
    ) throws QueryExecutionException {

        ChannelGroupQuery.Builder channelGroupQueryBuilder = ChannelGroupQuery.builder();
        boolean complexQuery = false; // Anything that doesn't require just querying for ids
        boolean refreshCache = false;
        Set<Id> channelGroupIds = null;
        NumberedChannelGroup.ChannelOrdering channelOrdering = NumberedChannelGroup.ChannelOrdering.CHANNEL_NUMBER;
        Set<Id> dttIds = null;
        Set<Id> ipIds = null;

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            String attributeName = attributeQuery.getAttributeName();
            List<?> attributeValue = attributeQuery.getValue();

            if (Attributes.CHANNEL_GROUP_IDS.externalName().equals(attributeName)) {
                channelGroupIds = ((List<Id>) attributeValue).stream()
                        .collect(MoreCollectors.toImmutableSet());
                channelGroupQueryBuilder.withChannelGroupIds(
                        channelGroupIds.stream()
                                .map(Id::longValue)
                                .collect(MoreCollectors.toImmutableSet())
                );
            } else if (Attributes.SOURCE.externalName().equals(attributeName)) {
                complexQuery = true;
                channelGroupQueryBuilder.withPublishers((List<Publisher>) attributeValue);
            } else if (Attributes.CHANNEL_GROUP_TYPE.externalName().equals(attributeName)) {
                complexQuery = true;
                channelGroupQueryBuilder.withTypes((List<String>) attributeValue);
            } else if (Attributes.REFRESH_CACHE.externalName().equals(attributeName)) {
                refreshCache = Boolean.parseBoolean(attributeValue.toString());
            } else if (attributeName.equals(Attributes.CHANNEL_GROUP_DTT_CHANNELS.externalName())) {
                Set<Id> newDttIds = ImmutableSet.copyOf((List<Id>) attributeValue);
                dttIds = dttIds == null ? newDttIds : Sets.union(dttIds, newDttIds);
            } else if (attributeName.equals(Attributes.CHANNEL_GROUP_IP_CHANNELS.externalName())) {
                Set<Id> newIpIds = ImmutableSet.copyOf((List<Id>) attributeValue);
                ipIds = ipIds == null ? newIpIds : Sets.union(ipIds, newIpIds);
            } else if (attributeName.equals(Attributes.CHANNEL_ORDERING.externalName())) {
                String orderingName = attributeValue.get(0).toString();
                channelOrdering = NumberedChannelGroup.ChannelOrdering.forName(orderingName);
                if (channelOrdering == null) {
                    throw new IllegalArgumentException(
                            "Invalid channel ordering: " + orderingName + " (possible values: " +
                                    NumberedChannelGroup.ChannelOrdering.names() + ")"
                    );
                }
            }
        }

        ChannelGroupQuery channelGroupQuery = channelGroupQueryBuilder.build();
        ListenableFuture<Resolved<ChannelGroup<?>>> resolvedChannelGroupsFuture;

        if (channelGroupIds != null && !complexQuery) {
            resolvedChannelGroupsFuture = channelGroupResolver.resolveIds(channelGroupIds, refreshCache);
        } else {
            resolvedChannelGroupsFuture = channelGroupResolver.resolveChannelGroups(channelGroupQuery);
        }

        Iterable<ChannelGroup<?>> resolvedChannelGroups = Futures.getChecked(
                Futures.transform(
                        resolvedChannelGroupsFuture,
                        (Resolved<ChannelGroup<?>> input) -> input.getResources()
                ),
                QueryExecutionException.class,
                1, TimeUnit.MINUTES
        );

        List<ChannelGroup<?>> filteredChannelGroups = MoreStreams.stream(resolvedChannelGroups)
                .filter(input -> query.getContext()
                        .getApplication()
                        .getConfiguration()
                        .isReadEnabled(input.getSource())
                )
                .collect(MoreCollectors.toImmutableList());

        ImmutableList<ChannelGroup<?>> selectedChannelGroups = query.getContext()
                .getSelection()
                .get()
                .applyTo(filteredChannelGroups);


        Iterable<ChannelGroup<?>> channelFilteredChannelGroups = getChannelFilteredChannelGroups(
                selectedChannelGroups,
                dttIds,
                ipIds
        );

        NumberedChannelGroup.ChannelOrdering finalChannelOrdering = channelOrdering;
        ImmutableList<ResolvedChannelGroup> channelGroupsResults = MoreStreams.stream(
                channelFilteredChannelGroups
        )
                .map(channelGroup ->
                        resolveAnnotationData(
                                query.getContext(),
                                channelGroup,
                                finalChannelOrdering
                        )
                )
                .collect(MoreCollectors.toImmutableList());

        return QueryResult.listResult(
                channelGroupsResults,
                query.getContext(),
                filteredChannelGroups.size()
        );
    }

    private ResolvedChannelGroup resolveAnnotationData(
            QueryContext ctxt,
            ChannelGroup<?> channelGroup,
            NumberedChannelGroup.ChannelOrdering channelOrdering
    ) {
        ResolvedChannelGroup.Builder resolvedChannelGroupBuilder =
                ResolvedChannelGroup.builder(channelGroup);

        resolvedChannelGroupBuilder.withRegionChannelGroups(
                contextHasAnnotation(ctxt, Annotation.REGIONS) ?
                    resolveRegionChannelGroups(channelGroup) :
                    Optional.empty()
        );

        resolvedChannelGroupBuilder.withPlatformChannelGroup(
                contextHasAnnotation(ctxt, Annotation.PLATFORM) ?
                    resolvePlatformChannelGroup(channelGroup) :
                    Optional.empty()
        );

        if (contextHasAnnotation(ctxt, Annotation.BASE_CHANNEL_GROUPS)) {
            resolvedChannelGroupBuilder.withChannelNumbersFromGroup(
                    resolveChannelNumbersFromChannelGroup(channelGroup)
            );
        } else {
            resolvedChannelGroupBuilder.withChannelNumbersFromGroup(Optional.empty());
        }

        if (contextHasAnnotation(ctxt, Annotation.FUTURE_CHANNELS)) {
            resolvedChannelGroupBuilder.withAdvertisedChannels(
                    resolveChannelsWithChannelGroups(
                            ctxt.getApplication()
                                    .getConfiguration(),
                            channelGroup,
                            contextHasAnnotation(
                                    ctxt,
                                    Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY
                            )
                            ? this::isChannelGroupMembership
                            : channelGroupMembership -> true,
                            true,
                            channelOrdering
                    )
            );
        } else if (contextHasAnnotation(ctxt, Annotation.CHANNEL_GROUPS_SUMMARY) ||
                contextHasAnnotation(ctxt, Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY)) {
            resolvedChannelGroupBuilder.withAdvertisedChannels(
                    resolveChannelsWithChannelGroups(
                            ctxt.getApplication()
                                    .getConfiguration(),
                            channelGroup,
                            contextHasAnnotation(
                                    ctxt,
                                    Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY
                            )
                            ? this::isChannelGroupMembership
                            : channelGroupMembership -> true,
                            false,
                            channelOrdering
                    )
            );
        } else if (contextHasAnnotation(ctxt, Annotation.ADVERTISED_CHANNELS) ||
                contextHasAnnotation(ctxt, Annotation.CHANNELS)) {
            boolean lcnSharing = contextHasAnnotation(ctxt, Annotation.LCN_SHARING);
            resolvedChannelGroupBuilder.withAdvertisedChannels(
                    resolveAdvertisedChannels(
                            channelGroup,
                            false,
                            lcnSharing,
                            channelOrdering
                    )
            );
        } else {
            resolvedChannelGroupBuilder.withAdvertisedChannels(Optional.empty());
        }

        return resolvedChannelGroupBuilder.build();
    }

    private Optional<Iterable<ChannelGroup<?>>> resolveRegionChannelGroups(ChannelGroup<?> entity) {

        if(!(entity instanceof  Platform)) {
            return Optional.empty();
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
            return Optional.empty();
        }

        Region region = (Region) entity;

        if (!region.getPlatform().isPresent()) {
            return Optional.empty();
        }

        Id platformId = region.getPlatform().get().getId();

        return Optional.ofNullable(Promise.wrap(channelGroupResolver.resolveIds(ImmutableSet.of(platformId)))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES)
                .first().orNull()
        );
    }

    private Optional<ChannelGroup<?>> resolveChannelNumbersFromChannelGroup(ChannelGroup<?> entity) {
        if (!(entity instanceof NumberedChannelGroup)) {
            return Optional.empty();
        }
        NumberedChannelGroup numberedChannelGroup = (NumberedChannelGroup) entity;
        if (!numberedChannelGroup.getChannelNumbersFrom().isPresent()) {
            return Optional.empty();
        }
        Id channelNumbersFromId = numberedChannelGroup.getChannelNumbersFrom().get().getId();

        return Optional.ofNullable(Promise.wrap(channelGroupResolver.resolveIds(ImmutableSet.of(channelNumbersFromId)))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES)
                .first().orNull()
        );
    }

    private Optional<Iterable<ResolvedChannel>> resolveChannelsWithChannelGroups(
            ApplicationConfiguration conf,
            ChannelGroup<?> entity,
            Function<ChannelGroupMembership, Boolean> whitelistedChannelGroupPredicate,
            boolean withFutureChannels,
            NumberedChannelGroup.ChannelOrdering channelOrdering
    ) {

        Optional<Iterable<ResolvedChannel>> channels = resolveAdvertisedChannels(
                entity,
                withFutureChannels,
                channelOrdering
        );
        if (!channels.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(
                StreamSupport.stream(channels.get().spliterator(), false)
                        .map(resolvedChannel -> ResolvedChannel.builder(resolvedChannel.getChannel())
                                .withChannelGroupSummaries(
                                        resolveChannelGroupSummaries(
                                                conf,
                                                resolvedChannel.getChannel(),
                                                whitelistedChannelGroupPredicate
                                        )
                                )
                                .withResolvedEquivalents(
                                        resolveChannelEquivalents(
                                                resolvedChannel.getChannel()
                                        )
                                )
                                .build()
                        )
                        .collect(Collectors.toList())
        );
    }

    private List<ChannelGroupSummary> resolveChannelGroupSummaries(
            ApplicationConfiguration conf,
            Channel channel,
            Function<ChannelGroupMembership, Boolean> whitelistedChannelGroupPredicate
    ) {
        Iterable<Id> channelGroupIds = channel.getChannelGroups().stream()
                .filter(whitelistedChannelGroupPredicate::apply)
                .map(ChannelGroupMembership::getChannelGroup)
                .map(ResourceRef::getId)
                .collect(MoreCollectors.toImmutableList());

        return resolveChannelGroupSummaries(conf, channelGroupIds);

    }

    private List<ChannelGroupSummary> resolveChannelGroupSummaries(
            ApplicationConfiguration conf,
            Iterable<Id> channelGroupIds
    ) {

        Iterable<ChannelGroup<?>> channelGroups =
                Promise.wrap(channelGroupResolver.resolveIds(channelGroupIds))
                        .then(Resolved::getResources)
                        .get(1, TimeUnit.MINUTES);

        return StreamSupport.stream(channelGroups.spliterator(), false)
                .filter(cg -> conf.isReadEnabled(cg.getSource()))
                .map(ChannelGroup::toSummary)
                .collect(MoreCollectors.toImmutableList());
    }

    private Optional<Iterable<ResolvedChannel>> resolveAdvertisedChannels(
            ChannelGroup<?> entity,
            boolean withFutureChannels,
            NumberedChannelGroup.ChannelOrdering channelOrdering
    ) {
        return resolveAdvertisedChannels(entity, withFutureChannels, false, channelOrdering);
    }

    private Optional<Iterable<ResolvedChannel>> resolveAdvertisedChannels(
            ChannelGroup<?> entity,
            boolean withFutureChannels,
            boolean lcnSharing,
            NumberedChannelGroup.ChannelOrdering channelOrdering
    ) {
        final ImmutableMultimap.Builder<Id, ChannelGroupMembership> builder = ImmutableMultimap.builder();

        Set<? extends ChannelGroupMembership> availableChannels;

        if (entity instanceof NumberedChannelGroup) {
            NumberedChannelGroup numberedChannelGroup = (NumberedChannelGroup) entity;
            if (withFutureChannels) {
                // N.B. futureChannels should effectively always allow lcnSharing, since future channel
                // might have same LCN as existing, to-be-deprecated channel.
                // This fetches future AND old, expired channels?
                availableChannels = numberedChannelGroup.getChannels(channelOrdering);
            } else {
                availableChannels = numberedChannelGroup.getChannelsAvailable(
                        LocalDate.now(),
                        channelOrdering,
                        lcnSharing
                );
            }
        } else {
            if (withFutureChannels) {
                availableChannels = entity.getChannels(); // This fetches future AND old, expired channels?
            } else {
                availableChannels = entity.getChannelsAvailable(LocalDate.now());
            }
        }



        List<Id> orderedIds = availableChannels.stream()
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

        Iterable<ResolvedChannel> sortedChannels =
                StreamSupport.stream(resolvedChannels.spliterator(), false)
                        .sorted((o1, o2) -> idOrdering.compare(o1.getId(), o2.getId()))
                        .map(channel -> ResolvedChannel.builder(channel)
                                .withResolvedEquivalents(resolveChannelEquivalents(channel))
                                .build()
                        )
                        .collect(Collectors.toList());

        return Optional.of(sortedChannels);
    }

    @Nullable
    private Iterable<Channel> resolveChannelEquivalents(Channel channel) {

        if (channel.getSameAs() == null || channel.getSameAs().isEmpty()) {
            return null;
        }

        Iterable<Id> ids = Iterables.transform(channel.getSameAs(), ResourceRef::getId);

        return Promise.wrap(channelResolver.resolveIds(ids))
                        .then(Resolved::getResources)
                        .get(1, TimeUnit.MINUTES);
    }

    private boolean contextHasAnnotation(QueryContext ctxt, Annotation annotation) {

        return (!Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
            &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        ).contains(annotation.toKey()));
    }

    private boolean isChannelGroupMembership(ChannelGroupMembership channelGroupMembership) {
        return !(channelGroupMembership instanceof ChannelNumbering);
    }

}
