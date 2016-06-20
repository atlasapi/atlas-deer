package org.atlasapi.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
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

import static com.google.common.base.Preconditions.checkArgument;

public class FiltersBuilder {

    public static TermsFilterBuilder buildForPublishers(String field,
            Iterable<Publisher> publishers) {
        return FilterBuilders.termsFilter(field, Iterables.transform(publishers, Publisher.TO_KEY));
    }

    public static TermsFilterBuilder buildForSpecializations(
            Iterable<Specialization> specializations) {
        return FilterBuilders.termsFilter(
                EsContent.SPECIALIZATION,
                Iterables.transform(specializations, Enum::name)
        );
    }

    public static FilterBuilder buildTopicIdFilter(List<List<InclusionExclusionId>> topicIdSets) {
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

    private static void addFilterForTopicId(BoolFilterBuilder filterBuilder,
            InclusionExclusionId id) {
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

    public static FilterBuilder buildAvailabilityFilter() {
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

    public static FilterBuilder buildActionableFilter(Map<String, String> actionableParams,
            Optional<Id> maybeRegionId,
            ChannelGroupResolver channelGroupResolver) {
        OrFilterBuilder orFilterBuilder = FilterBuilders.orFilter();
        if (actionableParams.get("location.available") != null) {
            orFilterBuilder.add(buildAvailabilityFilter());
        }
        DateTime broadcastTimeGreaterThan = actionableParams.get("broadcast.time.gt") == null ? null
                                                                                              : DateTime
                                                    .parse(actionableParams.get("broadcast.time.gt"));
        DateTime broadcastTimeLessThan = actionableParams.get("broadcast.time.lt") == null ? null
                                                                                           : DateTime
                                                 .parse(actionableParams.get("broadcast.time.lt"));
        if (broadcastTimeGreaterThan != null || broadcastTimeLessThan != null) {
            orFilterBuilder.add(
                    buildBroadcastRangeFilter(
                            broadcastTimeGreaterThan,
                            broadcastTimeLessThan,
                            maybeRegionId,
                            channelGroupResolver
                    )
            );
        }
        return orFilterBuilder;
    }

    public static FilterBuilder buildBroadcastRangeFilter(DateTime broadcastTimeGreaterThan,
            DateTime broadcastTimeLessThan,
            Optional<Id> maybeRegionId, ChannelGroupResolver cgResolver) {
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

        if (maybeRegionId.isPresent()) {
            FilterBuilder regionFilter = buildRegionFilter(maybeRegionId.get(), cgResolver);
            rangeFilter = rangeFilter.add(regionFilter);
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
    public static FilterBuilder buildRegionFilter(Id regionId,
            ChannelGroupResolver channelGroupResolver) {
        ChannelGroup region;
        try {
            Resolved<ChannelGroup<?>> resolved = Futures.get(
                    channelGroupResolver.resolveIds(ImmutableList.of(regionId)), IOException.class
            );
            region = resolved.getResources().first().get();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        ImmutableList<ChannelNumbering> channels = ImmutableList.copyOf(region.<ChannelNumbering>getChannels());
        ImmutableList<Long> channelsIdsForRegion = channels.stream()
                .map(c -> c.getChannel().getId())
                .map(Id::longValue)
                .collect(MoreCollectors.toImmutableList());

        return FilterBuilders.termsFilter(
                EsContent.BROADCASTS + "." + EsBroadcast.CHANNEL,
                channelsIdsForRegion
        ).cache(true);
    }
}
