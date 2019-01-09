package org.atlasapi.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.content.EsBroadcast;
import org.atlasapi.content.EsContent;
import org.atlasapi.content.EsLocation;
import org.atlasapi.content.InclusionExclusionId;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.EsTopic;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.collections.ListUtils;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.HasChildFilterBuilder;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkArgument;

public class FiltersBuilder {

    private FiltersBuilder() {
    }

    public static TermsFilterBuilder buildForPublishers(
            String field,
            Iterable<Publisher> publishers
    ) {
        return FilterBuilders.termsFilter(field, Iterables.transform(publishers, Publisher.TO_KEY));
    }

    public static TermsFilterBuilder buildForSpecializations(
            Iterable<Specialization> specializations
    ) {
        return FilterBuilders.termsFilter(
                EsContent.SPECIALIZATION,
                Iterables.transform(specializations, Enum::name)
        );
    }

    public static FilterBuilder buildTopicIdFilter(
            ImmutableList<ImmutableList<InclusionExclusionId>> topicIdSets
    ) {
        ImmutableList.Builder<FilterBuilder> topicIdFilters = ImmutableList.builder();
        for (List<InclusionExclusionId> idSet : topicIdSets) {
            BoolFilterBuilder filterForThisSet = FilterBuilders.boolFilter();
            for (InclusionExclusionId id : idSet) {
                addFilterForTopicId(filterForThisSet, id);
            }
            topicIdFilters.add(filterForThisSet);
        }
        OrFilterBuilder orFilter = FilterBuilders.orFilter();
        topicIdFilters.build().forEach(orFilter::add);
        return orFilter;
    }

    private static void addFilterForTopicId(
            BoolFilterBuilder filterBuilder,
            InclusionExclusionId id
    ) {
        NestedFilterBuilder filterForId = FilterBuilders.nestedFilter(
                EsContent.TOPICS + "." + EsTopic.TYPE_NAME,
                FilterBuilders.termFilter(
                        EsContent.TOPICS + "." + EsTopic.TYPE_NAME + "." + EsContent.ID,
                        id.getId()
                )
        );
        if (id.isExcluded()) {
            filterBuilder.mustNot(filterForId);
        } else {
            filterBuilder.must(filterForId);
        }
    }

    private static FilterBuilder buildAvailabilityFilter() {
        NestedFilterBuilder rangeFilter = FilterBuilders.nestedFilter(
                EsContent.LOCATIONS,
                FilterBuilders.andFilter(
                        FilterBuilders.rangeFilter(EsLocation.AVAILABILITY_TIME)
                                .lte(ElasticsearchUtils.clampDateToFloorMinute(DateTime.now())
                                        .toString()),
                        FilterBuilders.rangeFilter(EsLocation.AVAILABILITY_END_TIME)
                                .gte(ElasticsearchUtils.clampDateToFloorMinute(DateTime.now())
                                        .toString())
                )
        );
        return FilterBuilders.orFilter(
                rangeFilter,
                FilterBuilders.hasChildFilter(EsContent.CHILD_ITEM, rangeFilter)
        );
    }

    public static FilterBuilder buildActionableFilter(
            Map<String, String> actionableParams,
            Optional<List<Id>> maybeRegionIds,
            Optional<List<Id>> maybePlatformIds,
            Optional<List<Id>> maybeDttIds,
            Optional<List<Id>> maybeIpIds,
            ChannelGroupResolver channelGroupResolver
    ) {
        OrFilterBuilder orFilterBuilder = FilterBuilders.orFilter();
        if (actionableParams.get("location.available") != null) {
            orFilterBuilder.add(buildAvailabilityFilter());
        }
        DateTime broadcastTimeGreaterThan =
                actionableParams.get("broadcast.time.gt") == null ?
                null : DateTime.parse(actionableParams.get("broadcast.time.gt"));
        DateTime broadcastTimeLessThan =
                actionableParams.get("broadcast.time.lt") == null ?
                null : DateTime.parse(actionableParams.get("broadcast.time.lt"));
        if (broadcastTimeGreaterThan != null || broadcastTimeLessThan != null) {
            orFilterBuilder.add(
                    buildBroadcastRangeFilter(
                            broadcastTimeGreaterThan,
                            broadcastTimeLessThan,
                            maybeRegionIds,
                            maybePlatformIds,
                            maybeDttIds,
                            maybeIpIds,
                            channelGroupResolver
                    )
            );
        }
        return orFilterBuilder;
    }

    private static FilterBuilder buildBroadcastRangeFilter(
            DateTime broadcastTimeGreaterThan,
            DateTime broadcastTimeLessThan,
            Optional<List<Id>> maybeRegionIds,
            Optional<List<Id>> maybePlatformIds,
            Optional<List<Id>> maybeDttIds,
            Optional<List<Id>> maybeIpIds,
            ChannelGroupResolver cgResolver
    ) {
        if (broadcastTimeGreaterThan != null && broadcastTimeLessThan != null) {
            checkArgument(
                    !broadcastTimeGreaterThan.isAfter(broadcastTimeLessThan),
                    "Invalid range in actionableFilterParameters, broadcast.time.gt cannot be "
                            + "after broadcast.time.lt"
            );
        }

        RangeFilterBuilder startTimeFilter = FilterBuilders.rangeFilter(
                EsContent.BROADCASTS + "." + EsBroadcast.TRANSMISSION_TIME
        );
        RangeFilterBuilder endTimeFilter = FilterBuilders.rangeFilter(
                EsContent.BROADCASTS + "." + EsBroadcast.TRANSMISSION_END_TIME
        );

        // Query for broadcast times that are at least partially contained between
        // broadcastTimeGreaterThan and broadcastTimeLessThan
        if (broadcastTimeGreaterThan != null) {
            endTimeFilter.gte(ElasticsearchUtils.clampDateToFloorMinute(broadcastTimeGreaterThan)
                    .toString());
        }
        if (broadcastTimeLessThan != null) {
            startTimeFilter.lte(ElasticsearchUtils.clampDateToFloorMinute(broadcastTimeLessThan)
                    .toString());
        }

        AndFilterBuilder rangeFilter = FilterBuilders.andFilter(startTimeFilter, endTimeFilter);

        List<Id> channelGroupsIds = Lists.newArrayList();
        maybeRegionIds.ifPresent(channelGroupsIds::addAll);
        maybePlatformIds.ifPresent(channelGroupsIds::addAll);

        if (!channelGroupsIds.isEmpty()) {
            FilterBuilder channelGroupFilter = buildChannelGroupFilter(
                    channelGroupsIds,
                    maybeDttIds,
                    maybeIpIds,
                    cgResolver
            );
            rangeFilter = rangeFilter.add(channelGroupFilter);
        }

        NestedFilterBuilder parentFilter = FilterBuilders.nestedFilter(
                EsContent.BROADCASTS,
                rangeFilter
        );
        HasChildFilterBuilder childFilter = FilterBuilders.hasChildFilter(
                EsContent.CHILD_ITEM,
                parentFilter
        );

        return FilterBuilders.orFilter(parentFilter, childFilter).cache(true);
    }

    public static FilterBuilder getSeriesIdFilter(Id id, SecondaryIndex equivIdIndex) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return FilterBuilders.termsFilter(EsContent.SERIES, ids).cache(true);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static FilterBuilder getBrandIdFilter(Id id, SecondaryIndex equivIdIndex) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return FilterBuilders.termsFilter(EsContent.BRAND, ids).cache(true);
        } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
        }
    }

    @SuppressWarnings("unchecked")
    private static FilterBuilder buildChannelGroupFilter(
            List<Id> channelGroupIds,
            Optional<List<Id>> dttIds,
            Optional<List<Id>> ipIds,
            ChannelGroupResolver channelGroupResolver
    ) {
        List<ChannelGroup> channelGroups = resolveChannelGroups(
                channelGroupIds,
                channelGroupResolver
        );

        List<ChannelNumbering> channels = filterChannelsByDttOrIp(dttIds, ipIds, channelGroups);
        ImmutableList<Long> channelsIdsForChannelGroup = channels.stream()
                .map(c -> c.getChannel().getId())
                .map(Id::longValue)
                .collect(MoreCollectors.toImmutableList());

        return FilterBuilders.termsFilter(
                EsContent.BROADCASTS + "." + EsBroadcast.CHANNEL,
                channelsIdsForChannelGroup
        ).cache(true);
    }

    private static List<ChannelGroup> resolveChannelGroups(
            List<Id> channelGroupIds,
            ChannelGroupResolver channelGroupResolver
    ) {
        List<ChannelGroup> channelGroups;
        try {
            Iterable<ChannelGroup<?>> resolvedChannelGroups = Futures.get(
                    Futures.transform(
                            channelGroupResolver.resolveIds(channelGroupIds),
                            (Resolved<ChannelGroup<?>> input) -> input.getResources()
                    ),
                    1, TimeUnit.MINUTES,
                    IOException.class
            );
            channelGroups = Lists.newArrayList(resolvedChannelGroups);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return channelGroups;
    }

    private static List<ChannelNumbering> filterChannelsByDttOrIp(
            Optional<List<Id>> dttIds,
            Optional<List<Id>> ipIds,
            List<ChannelGroup> channelGroups
    ) {
        List<ChannelNumbering> channels = Lists.newArrayList();
        channelGroups.forEach(region -> {
            ImmutableList<ChannelNumbering> allChannels = ImmutableList.copyOf(
                    region.getChannelsAvailable(LocalDate.now())
            );
            if (dttIds.isPresent() && dttIds.get().contains(region.getId())) {
                ImmutableSet<ChannelNumbering> dttChannels = allChannels.stream()
                        .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                        .filter(channel -> Integer.parseInt(channel.getChannelNumber().get())
                                <= 300)
                        .collect(MoreCollectors.toImmutableSet());

                channels.addAll(dttChannels);
            } else if (ipIds.isPresent() && ipIds.get().contains(region.getId())) {
                ImmutableSet<ChannelNumbering> ipChannels = allChannels.stream()
                        .filter(channel -> !Strings.isNullOrEmpty(channel.getChannelNumber().get()))
                        .filter(channel -> Integer.parseInt(channel.getChannelNumber().get()) > 300)
                        .collect(MoreCollectors.toImmutableSet());

                channels.addAll(ipChannels);
            } else {
                channels.addAll(allChannels);
            }
        });
        return channels;
    }
}
