package org.atlasapi.query.v4.channelgroup;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ChannelResolver;
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

import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.promise.Promise;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import org.joda.time.LocalDate;

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
                        channelGroupResolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        (Resolved<ChannelGroup<?>> resolved) -> {
                            if (resolved.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(
                                        new NotFoundException(query.getOnlyId())
                                );
                            }

                            ChannelGroup channelGroup = resolved.getResources()
                                    .first()
                                    .get();

                            ResolvedChannelGroup resolvedChannelGroup = resolveAnnotationData(
                                    query.getContext(),
                                    channelGroup
                            );

                            boolean queryHasOperands = !query.getOperands().isEmpty();
                            if (queryHasOperands) {
                                for (AttributeQuery<?> attributeQuery : query.getOperands()) {
                                    if (attributeQuery.getAttributeName()
                                            .equals(Attributes.CHANNEL_GROUP_DTT_CHANNELS.externalName())) {
                                        List<String> dttIds = getChannelIdsFromQuery(attributeQuery);
                                        if (!dttIds.isEmpty() && dttIds.contains(idCodec.encode(
                                                channelGroup.getId().toBigInteger()))) {
                                            filterDttChannels(channelGroup);
                                        }
                                    }

                                    if (attributeQuery.getAttributeName()
                                            .equals(Attributes.CHANNEL_GROUP_IP_CHANNELS.externalName())) {
                                        List<String> ipIds = getChannelIdsFromQuery(attributeQuery);
                                        if (!ipIds.isEmpty() && ipIds.contains(idCodec.encode(
                                                channelGroup.getId().toBigInteger()))) {
                                            filterIpChannels(channelGroup);
                                        }
                                    }
                                }
                            }

                            // This is a temporary hack for testing purposes. We do not want to show
                            // the new duplicate BT channels that will have a start date somewhere 5
                            // years in the future. This should be removed once we deliver the
                            // channel grouping tool
                            if (query.getContext()
                                    .getApplication()
                                    .getTitle()
                                    .equals("BT TVE Prod")) {

                                if (queryHasOperands) {
                                    for (AttributeQuery<?> attributeQuery : query.getOperands()) {
                                        if (attributeQuery.getAttributeName()
                                                .equals(Attributes.CHANNEL_GROUP_FUTURE_CHANNELS.externalName())) {
                                            if (!Boolean.getBoolean(attributeQuery.getValue()
                                                    .get(0)
                                                    .toString())) {
                                                return QueryResult.singleResult(
                                                        resolvedChannelGroup,
                                                        query.getContext()
                                                );
                                            }
                                        }
                                    }

                                    ImmutableSet<? extends ChannelGroupMembership> availableChannels = ImmutableSet.copyOf(
                                            channelGroup.getChannelsAvailable(LocalDate.now())
                                    );
                                    channelGroup.setChannels(availableChannels);
                                }
                            }

                            return QueryResult.singleResult(
                                    resolvedChannelGroup,
                                    query.getContext()
                            );
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );
    }

    private void filterIpChannels(ChannelGroup channelGroup) {
        ImmutableSet<ChannelNumbering> channels = ImmutableSet.copyOf(channelGroup.getChannels());

        ImmutableSet<ChannelNumbering> ipChannels = channels.stream()
                .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                .filter(channel -> Integer.parseInt(channel.getChannelNumber().get()) > 300)
                .collect(MoreCollectors.toImmutableSet());

        channelGroup.setChannels(ipChannels);
    }

    private void filterDttChannels(ChannelGroup channelGroup) {
        ImmutableSet<ChannelNumbering> immutableSet = ImmutableSet.copyOf(channelGroup.getChannels());

        ImmutableSet<ChannelNumbering> dttChannels = immutableSet.stream()
                .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                .filter(channel -> Integer.parseInt(channel.getChannelNumber().get()) <= 300)
                .collect(MoreCollectors.toImmutableSet());

        channelGroup.setChannels(dttChannels);
    }

    private QueryResult<ResolvedChannelGroup> executeListQuery(Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        Iterable<ChannelGroup<?>> channelGroups = Futures.get(
                Futures.transform(
                        channelGroupResolver.allChannels(),
                        (Resolved<ChannelGroup<?>> input) -> input.getResources()
                ),
                1, TimeUnit.MINUTES,
                QueryExecutionException.class
        );

        channelGroups = query.getContext()
                .getSelection()
                .get()
                .applyTo(channelGroups);

        channelGroups = StreamSupport.stream(channelGroups.spliterator(), false)
                .filter(input -> query.getContext()
                        .getApplication()
                        .getConfiguration()
                        .isReadEnabled(input.getSource())
                )
                .collect(Collectors.toList());

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_TYPE.externalName())) {
                final String channelGroupType = attributeQuery.getValue().get(0).toString();
                channelGroups = StreamSupport.stream(channelGroups.spliterator(), false)
                        .filter(channelGroup -> channelGroupType.equals(channelGroup.getType()))
                        .collect(Collectors.toList());
            }

            if (attributeQuery.getAttributeName()
                    .equals(Attributes.SOURCE.externalName())) {
                channelGroups = StreamSupport.stream(channelGroups.spliterator(), false)
                        .filter(channelGroup -> channelGroup.getSource()
                                .key()
                                .equals(attributeQuery.getValue().get(0).toString())
                        )
                        .collect(Collectors.toList());
            }

            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_DTT_CHANNELS.externalName())) {
                List<String> dttIds = getChannelIdsFromQuery(attributeQuery);
                channelGroups.forEach(channelGroup -> {
                    if (dttIds.contains(idCodec.encode(channelGroup.getId().toBigInteger()))) {
                        filterDttChannels(channelGroup);
                    }
                });
            }

            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_IP_CHANNELS.externalName())) {
                List<String> ipIds = getChannelIdsFromQuery(attributeQuery);
                channelGroups.forEach(channelGroup -> {
                    if (ipIds.contains(idCodec.encode(channelGroup.getId().toBigInteger()))) {
                        filterIpChannels(channelGroup);
                    }
                });
            }
        }

        ImmutableList<ResolvedChannelGroup> resolvedChannelGroups =
                StreamSupport.stream(channelGroups.spliterator(), false)
                        .map(channelGroup -> resolveAnnotationData(query.getContext(), channelGroup))
                        .collect(MoreCollectors.toImmutableList());

        return QueryResult.listResult(
                resolvedChannelGroups,
                query.getContext(),
                resolvedChannelGroups.size()
        );
    }

    private List<String> getChannelIdsFromQuery(AttributeQuery<?> attributeQuery) {
        return Arrays.asList(attributeQuery.getValue()
                .get(0)
                .toString()
                .split("\\s*,\\s*"));
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
                    Optional.empty()
        );

        resolvedChannelGroupBuilder.withPlatformChannelGroup(
                contextHasAnnotation(ctxt, Annotation.PLATFORM) ?
                    resolvePlatformChannelGroup(channelGroup) :
                    Optional.empty()
        );

        if (contextHasAnnotation(ctxt, Annotation.CHANNEL_GROUPS_SUMMARY) ||
                contextHasAnnotation(ctxt, Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY)) {
            resolvedChannelGroupBuilder.withAdvertisedChannels(
                    resolveChannelsWithChannelGroups(
                            ctxt.getApplication().getConfiguration(),
                            channelGroup,
                            contextHasAnnotation(
                                    ctxt,
                                    Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY
                            )
                            ? this::isChannelGroupMembership
                            : channelGroupMembership -> true
                    )
            );
        } else if (contextHasAnnotation(ctxt, Annotation.ADVERTISED_CHANNELS) ||
            contextHasAnnotation(ctxt, Annotation.CHANNELS)) {
            resolvedChannelGroupBuilder.withAdvertisedChannels(
                    resolveAdvertisedChannels(channelGroup)
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

    private Optional<Iterable<ResolvedChannel>> resolveChannelsWithChannelGroups(
            ApplicationConfiguration conf,
            ChannelGroup<?> entity,
            Function<ChannelGroupMembership, Boolean> whitelistedChannelGroupPredicate
    ) {
        Optional<Iterable<ResolvedChannel>> channels = resolveAdvertisedChannels(entity);
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

    private Optional<Iterable<ResolvedChannel>> resolveAdvertisedChannels(ChannelGroup<?> entity) {

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
