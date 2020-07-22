package org.atlasapi.util;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.content.InclusionExclusionId;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.sherlock.client.search.helpers.OccurrenceClause;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.Parameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.SingleMappingBoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermsParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkArgument;

public class FiltersBuilder {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContent();

    private FiltersBuilder() {
    }

    public static TermsParameter<String> buildForPublishers(
            KeywordMapping<String> publisherMapping,
            Iterable<Publisher> publishers
    ) {
        return TermsParameter.of(
                publisherMapping,
                Iterables.transform(publishers, Publisher.TO_KEY)
        );
    }

    public static TermsParameter<String> buildForSpecializations(
            Iterable<Specialization> specializations
    ) {
        return TermsParameter.of(
                CONTENT_MAPPING.getSpecialization(),
                Iterables.transform(specializations, Specialization::toString)
        );
    }

    public static BoolParameter buildTopicIdFilter(
            ImmutableList<ImmutableList<InclusionExclusionId>> topicIdSets
    ) {
        List<BoolParameter> topicIdParameters = new ArrayList<>();

        for (List<InclusionExclusionId> idSet : topicIdSets) {

            SingleMappingBoolParameter.Builder<Long> boolParameterBuilder =
                    SingleMappingBoolParameter.builder(CONTENT_MAPPING.getTags().getId());

            for (InclusionExclusionId id : idSet) {
                if (id.isIncluded()) {
                    boolParameterBuilder.addValue(id.getId().longValue(), OccurrenceClause.MUST);
                } else {
                    boolParameterBuilder.addValue(id.getId().longValue(), OccurrenceClause.MUST_NOT);
                }
            }
            topicIdParameters.add(boolParameterBuilder.build());
        }

        return new SingleClauseBoolParameter(topicIdParameters, OccurrenceClause.SHOULD);
    }

    private static BoolParameter buildAvailabilityFilter() {

        return SingleClauseBoolParameter.must(
                RangeParameter.to(
                        CONTENT_MAPPING.getLocations().getAvailabilityStart(),
                        Instant.now().truncatedTo(ChronoUnit.MINUTES)),
                RangeParameter.from(
                        CONTENT_MAPPING.getLocations().getAvailabilityEnd(),
                        Instant.now().truncatedTo(ChronoUnit.MINUTES))
        );

        // TODO look for children with availability too
//        return FilterBuilders.orFilter(
//                rangeFilter,
//                FilterBuilders.hasChildFilter(EsContent.CHILD_ITEM, rangeFilter)
//        );
    }

    public static BoolParameter buildActionableFilter(
            Map<String, String> actionableParams,
            Optional<List<Id>> maybeRegionIds,
            Optional<List<Id>> maybePlatformIds,
            Optional<List<Id>> maybeDttIds,
            Optional<List<Id>> maybeIpIds,
            ChannelGroupResolver channelGroupResolver
    ) {
        List<Parameter> parameters = new ArrayList<>();

        if (actionableParams.get("location.available") != null) {
            parameters.add(buildAvailabilityFilter());
        }
        Instant broadcastTimeGreaterThan = actionableParams.get("broadcast.time.gt") == null
                ? null
                : Instant.ofEpochMilli(
                        DateTime.parse(actionableParams.get("broadcast.time.gt")).getMillis()
                );
        Instant broadcastTimeLessThan = actionableParams.get("broadcast.time.lt") == null
                ? null
                : Instant.ofEpochMilli(
                        DateTime.parse(actionableParams.get("broadcast.time.lt")).getMillis()
                );
        if (broadcastTimeGreaterThan != null || broadcastTimeLessThan != null) {
            parameters.add(
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

        return SingleClauseBoolParameter.should(parameters);
    }

    private static BoolParameter buildBroadcastRangeFilter(
            Instant broadcastTimeGreaterThan,
            Instant broadcastTimeLessThan,
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

        List<Parameter> parentParameters = new ArrayList<>();

        // Query for broadcast times that are at least partially contained between
        // broadcastTimeGreaterThan and broadcastTimeLessThan
        if (broadcastTimeGreaterThan != null) {
            parentParameters.add(RangeParameter.from(
                    CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                    broadcastTimeGreaterThan.truncatedTo(ChronoUnit.MINUTES)
            ));
        }
        if (broadcastTimeLessThan != null) {
            parentParameters.add(RangeParameter.to(
                    CONTENT_MAPPING.getBroadcasts().getTransmissionStartTime(),
                    broadcastTimeLessThan.truncatedTo(ChronoUnit.MINUTES)
            ));
        }

        List<Id> channelGroupsIds = Lists.newArrayList();
        maybeRegionIds.ifPresent(channelGroupsIds::addAll);
        maybePlatformIds.ifPresent(channelGroupsIds::addAll);

        if (!channelGroupsIds.isEmpty()) {
            TermsParameter<Long> channelGroupFilter = buildChannelGroupFilter(
                    channelGroupsIds,
                    maybeDttIds,
                    maybeIpIds,
                    cgResolver
            );
            parentParameters.add(channelGroupFilter);
        }

        // TODO add has child query
//        HasChildFilterBuilder childFilter = FilterBuilders.hasChildFilter(
//                EsContent.CHILD_ITEM,
//                parentFilter
//        );
//        FilterBuilders.orFilter(parentFilter, childFilter).cache(true);
        return SingleClauseBoolParameter.should(
                SingleClauseBoolParameter.must(parentParameters)
                // TODO add child parameters
        );

    }

    public static TermsParameter<Long> getSeriesIdFilter(Id id, SecondaryIndex equivIdIndex) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return TermsParameter.of(CONTENT_MAPPING.getSeries().getId(), ids);
            //return FilterBuilders.termsFilter(EsContent.SERIES, ids).cache(true);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static TermsParameter<Long> getBrandIdFilter(Id id, SecondaryIndex equivIdIndex) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return TermsParameter.of(CONTENT_MAPPING.getBrand().getId(), ids);
            //return FilterBuilders.termsFilter(EsContent.BRAND, ids).cache(true);
        } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
        }
    }

    @SuppressWarnings("unchecked")
    private static TermsParameter<Long> buildChannelGroupFilter(
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

        return TermsParameter.of(
                CONTENT_MAPPING.getBroadcasts().getBroadcastOn(),
                channelsIdsForChannelGroup
        );
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
