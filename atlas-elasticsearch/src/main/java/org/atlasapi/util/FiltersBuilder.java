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
import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.EsTopic;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.joda.time.DateTime;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

public class FiltersBuilder {

    public static TermsFilterBuilder buildForPublishers(String field, Iterable<Publisher> publishers) {
        return FilterBuilders.termsFilter(field, Iterables.transform(publishers, Publisher.TO_KEY));
    }

    public static TermsFilterBuilder buildForSpecializations(Iterable<Specialization> specializations) {
        return FilterBuilders.termsFilter(
                EsContent.SPECIALIZATION,
                Iterables.transform(specializations, input -> input.name())
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

    private static void addFilterForTopicId(BoolFilterBuilder filterBuilder, InclusionExclusionId id) {
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
                                .lte(DateTime.now().toString()),
                        FilterBuilders.rangeFilter(EsLocation.AVAILABILITY_END_TIME)
                                .gte(DateTime.now().toString()))
        );
        return FilterBuilders.orFilter(
                rangeFilter,
                FilterBuilders.hasChildFilter(EsContent.CHILD_ITEM, rangeFilter)
        );
    }

    public static FilterBuilder builderActionableFilter(Map<String, String> actionableParams) {
        OrFilterBuilder orFilterBuilder = FilterBuilders.orFilter();
        if (actionableParams.get("location.available") != null) {
            orFilterBuilder.add(buildAvailabilityFilter());
        }
        DateTime broadcastTimeGreaterThan = actionableParams.get("broadcast.time.gt") == null ? null
                : DateTime.parse(actionableParams.get("broadcast.time.gt"));
        DateTime broadcastTimeLessThan = actionableParams.get("broadcast.time.lt") == null ? null
                : DateTime.parse(actionableParams.get("broadcast.time.lt"));
        if (broadcastTimeGreaterThan != null || broadcastTimeLessThan != null) {
            orFilterBuilder.add(
                    buildBroadcastRangeFilter(broadcastTimeGreaterThan, broadcastTimeLessThan)
            );
        }
        return orFilterBuilder;
    }

    public static FilterBuilder buildBroadcastRangeFilter(DateTime broadcastTimeGreaterThan, DateTime broadcastTimeLessThan) {
        RangeFilterBuilder parentFilter = FilterBuilders.rangeFilter("transmissionTimeInMillis");
        if (broadcastTimeGreaterThan != null) {
            parentFilter.gte(broadcastTimeGreaterThan.getMillis());
        }
        if (broadcastTimeLessThan != null) {
            parentFilter.lte(broadcastTimeLessThan.getMillis());
        }
        return FilterBuilders.orFilter(
                parentFilter,
                FilterBuilders.hasChildFilter(EsContent.CHILD_ITEM, parentFilter)
        );
    }

    public static FilterBuilder getSeriesIdFilter(Id id, SecondaryIndex equivIdIndex) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return FilterBuilders.termsFilter(EsContent.SERIES, ids);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static FilterBuilder getBrandIdFilter(Id id, SecondaryIndex equivIdIndex) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return FilterBuilders.termsFilter(EsContent.BRAND, ids);
        } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
        }
    }

    @SuppressWarnings("unchecked")
    public static FilterBuilder addRegionFilter(Optional<IndexQueryParams> queryParams, ChannelGroupResolver channelGroupResolver) {
        Id regionId = queryParams.get().getRegionId().get();
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
                .collect(ImmutableCollectors.toList());

        return FilterBuilders.nestedFilter(
                EsContent.BROADCASTS,
                FilterBuilders.termsFilter(EsContent.BROADCASTS + "." + EsBroadcast.CHANNEL, channelsIdsForRegion)
        );
    }
}
