package org.atlasapi.query.v4.channelgroup;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
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

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
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
                                Boolean.parseBoolean(query.getContext().getRequest().getParameter(
                                        Attributes.REFRESH_CACHE_PARAM))
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

                            NumberedChannelGroup.ChannelOrdering channelOrdering = filterChannelsAndGetOrdering(
                                    ImmutableList.of(channelGroup),
                                    query.getOperands()
                            );

                            ResolvedChannelGroup resolvedChannelGroup = resolveAnnotationData(
                                    query.getContext(),
                                    channelGroup,
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

    private NumberedChannelGroup.ChannelOrdering filterChannelsAndGetOrdering(
            Iterable<ChannelGroup<?>> channelGroups,
            Iterable<AttributeQuery<?>> attributeQueries
    ) {
        NumberedChannelGroup.ChannelOrdering ordering = NumberedChannelGroup.ChannelOrdering.CHANNEL_NUMBER;
        Set<Id> dttIds = null;
        Set<Id> ipIds = null;
        for (AttributeQuery<?> attributeQuery : attributeQueries) {
            String attributeName = attributeQuery.getAttributeName();
            if (attributeName.equals(Attributes.CHANNEL_GROUP_DTT_CHANNELS.externalName())) {
                dttIds = ImmutableSet.copyOf((List<Id>) attributeQuery.getValue());
            } else if (attributeName.equals(Attributes.CHANNEL_GROUP_IP_CHANNELS.externalName())) {
                ipIds = ImmutableSet.copyOf((List<Id>) attributeQuery.getValue());
            } else if (attributeName.equals(Attributes.CHANNEL_ORDERING.externalName())) {
                String orderingName = attributeQuery.getValue().get(0).toString();
                ordering = NumberedChannelGroup.ChannelOrdering.forName(orderingName);
                if (ordering == null) {
                    throw new IllegalArgumentException(
                            "Invalid channel ordering: " + orderingName + " (possible values: " +
                                    NumberedChannelGroup.ChannelOrdering.names() + ")"
                    );
                }
            }
        }
        for (ChannelGroup<?> channelGroup : channelGroups) {
            if (channelGroup instanceof NumberedChannelGroup) {
                NumberedChannelGroup numberedChannelGroup = (NumberedChannelGroup) channelGroup;
                if (dttIds != null && !dttIds.isEmpty() && dttIds.contains(channelGroup.getId())) {
                    filterDttChannels(numberedChannelGroup);
                }
                if (ipIds != null && !ipIds.isEmpty() && ipIds.contains(channelGroup.getId())) {
                    filterIpChannels(numberedChannelGroup);
                }
            }
        }
        return ordering;
    }

    private void filterIpChannels(NumberedChannelGroup channelGroup) {
        ImmutableSet<ChannelNumbering> ipChannels = StreamSupport.stream(
                channelGroup.getChannels(NumberedChannelGroup.ChannelOrdering.SPECIFIED).spliterator(),
                false
        )
                .filter(channel -> channel.getChannelNumber().isPresent())
                .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                .filter(channel -> Integer.parseInt(channel.getChannelNumber().get()) > 300)
                .collect(MoreCollectors.toImmutableSet());

        channelGroup.setChannels(ipChannels);
    }

    private void filterDttChannels(NumberedChannelGroup channelGroup) {
        ImmutableSet<ChannelNumbering> dttChannels = StreamSupport.stream(
                channelGroup.getChannels(NumberedChannelGroup.ChannelOrdering.SPECIFIED).spliterator(),
                false
        )
                .filter(channel -> channel.getChannelNumber().isPresent())
                .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                .filter(channel -> Integer.parseInt(channel.getChannelNumber().get()) <= 300)
                .collect(MoreCollectors.toImmutableSet());

        channelGroup.setChannels(dttChannels);
    }

    private QueryResult<ResolvedChannelGroup> executeListQuery(Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        Iterable<ChannelGroup<?>> resolvedChannelGroups;
        List<Id> lids = Lists.newArrayList();

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_IDS.externalName())) {
                lids = (List<Id>) attributeQuery.getValue();
            }
        }

        if (lids.isEmpty()) {
            resolvedChannelGroups = Futures.get(
                    Futures.transform(
                            channelGroupResolver.allChannels(),
                            (Resolved<ChannelGroup<?>> input) -> input.getResources()
                    ),
                    1, TimeUnit.MINUTES,
                    QueryExecutionException.class
            );
        } else {
            resolvedChannelGroups = Futures.get(
                    Futures.transform(
                            channelGroupResolver.resolveIds(
                                    lids,
                                    Boolean.parseBoolean(query.getContext()
                                            .getRequest()
                                            .getParameter(Attributes.REFRESH_CACHE_PARAM)
                                    )
                            ),
                            (Resolved<ChannelGroup<?>> input) -> input.getResources()
                    ),
                    1, TimeUnit.MINUTES,
                    QueryExecutionException.class
            );
        }

        List<ChannelGroup<?>> channelGroups = Lists.newArrayList(resolvedChannelGroups);

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_TYPE.externalName())) {
                final String channelGroupType = attributeQuery.getValue().get(0).toString();
                channelGroups = channelGroups.stream()
                        .filter(channelGroup -> channelGroupType.equals(channelGroup.getType()))
                        .collect(Collectors.toList());
            }

            if (attributeQuery.getAttributeName()
                    .equals(Attributes.SOURCE.externalName())) {
                channelGroups = channelGroups.stream()
                        .filter(channelGroup -> channelGroup.getSource()
                                .key()
                                .equals(attributeQuery.getValue().get(0).toString())
                        )
                        .collect(Collectors.toList());
            }
        }

        channelGroups = query.getContext()
                .getSelection()
                .get()
                .applyTo(channelGroups);

        channelGroups = channelGroups.stream()
                .filter(input -> query.getContext()
                        .getApplication()
                        .getConfiguration()
                        .isReadEnabled(input.getSource())
                )
                .collect(Collectors.toList());

        NumberedChannelGroup.ChannelOrdering channelOrdering = filterChannelsAndGetOrdering(
                channelGroups,
                query.getOperands()
        );

        ImmutableList<ResolvedChannelGroup> channelGroupsResults = channelGroups.stream()
                        .map(channelGroup -> resolveAnnotationData(query.getContext(), channelGroup, channelOrdering))
                        .collect(MoreCollectors.toImmutableList());

        return QueryResult.listResult(
                channelGroupsResults,
                query.getContext(),
                channelGroupsResults.size()
        );
    }

    private List<String> getIdsListFromAttribute(AttributeQuery<?> attributeQuery) {
        return attributeQuery.getValue()
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());
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

        Id platformId = ((Region) entity).getPlatform().getId();

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

        Iterable<? extends ChannelGroupMembership> availableChannels;

        if (entity instanceof NumberedChannelGroup) {
            NumberedChannelGroup numberedChannelGroup = (NumberedChannelGroup) entity;
            if (withFutureChannels) {
                // N.B. futureChannels should effectively always allow lcnSharing, since future channel
                // might have same LCN as existing, to-be-deprecated channel
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
                // N.B. futureChannels should effectively always allow lcnSharing, since future channel
                // might have same LCN as existing, to-be-deprecated channel
                availableChannels = entity.getChannels();   // this fetches future AND old, expired channels?
            } else {
                availableChannels = entity.getChannelsAvailable(LocalDate.now(), lcnSharing);
            }
        }



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
