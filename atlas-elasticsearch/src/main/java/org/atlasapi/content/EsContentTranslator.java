package org.atlasapi.content;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.atlasapi.entity.Id;
import org.atlasapi.util.ElasticsearchUtils;
import org.atlasapi.util.EsObject;
import org.atlasapi.util.SecondaryIndex;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class EsContentTranslator {

    public static final DateTime AVAILABLE_FROM_FOREVER =
            new DateTime(1900, 1, 1, 0, 0, DateTimeZone.UTC);
    public static final DateTime AVAILABLE_UNTIL_FOREVER =
            new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC);

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Client esClient;
    private final SecondaryIndex equivIdIndex;
    private final String indexName;
    private final Integer requestTimeout;

    public EsContentTranslator(
            String indexName,
            Client esClient,
            SecondaryIndex equivIdIndex,
            Long requestTimeout
    ) {
        checkArgument(!Strings.isNullOrEmpty(indexName), "Index name cannot be null or empty");
        this.indexName = indexName;
        this.esClient = checkNotNull(esClient);
        this.equivIdIndex = checkNotNull(equivIdIndex);
        this.requestTimeout = checkNotNull(requestTimeout.intValue());
    }

    private Collection<EsBroadcast> makeESBroadcasts(Item item) {
        Collection<EsBroadcast> esBroadcasts = new LinkedList<>();
        for (Broadcast broadcast : item.getBroadcasts()) {
            if (broadcast.isActivelyPublished()) {
                esBroadcasts.add(toEsBroadcast(broadcast));
            }
        }
        return esBroadcasts;
    }

    private EsBroadcast toEsBroadcast(Broadcast broadcast) {
        return new EsBroadcast()
                .id(broadcast.getSourceId())
                .channel(broadcast.getChannelId().longValue())
                .transmissionTime(toUtc(broadcast.getTransmissionTime()).toDate())
                .transmissionEndTime(toUtc(broadcast.getTransmissionEndTime()).toDate())
                .repeat(broadcast.getRepeat() != null ? broadcast.getRepeat() : false);
    }

    private Iterable<EsPriceMapping> makeEsPrices(Set<Encoding> manifestedAs) {
        if (manifestedAs == null) {
            return ImmutableList.of();
        }
        return manifestedAs.stream()
                .flatMap(encoding -> encoding.getAvailableAt().stream())
                .filter(p -> p != null)
                .map(Location::getPolicy)
                .filter(p -> p != null)
                .map(Policy::getPrice)
                .filter(p -> p != null && p.getCurrency() != null)
                .map(price -> new EsPriceMapping().currency(price.getCurrency())
                        .value(price.getAmount()))
                .collect(MoreCollectors.toImmutableList());
    }

    private DateTime toUtc(DateTime transmissionTime) {
        if (transmissionTime == null) {
            return null;
        }
        return transmissionTime.toDateTime(DateTimeZones.UTC);
    }

    private EsLocation toEsLocation(Location location) {
        @SuppressWarnings("ConstantConditions")
        @Nonnull Policy policy = location.getPolicy();

        EsLocation esLocation = new EsLocation();

        DateTime availabilityStart = policy.getAvailabilityStart() != null
                                     ? policy.getAvailabilityStart()
                                     : AVAILABLE_FROM_FOREVER;

        DateTime availabilityEnd = policy.getAvailabilityEnd() != null
                                   ? policy.getAvailabilityEnd()
                                   : AVAILABLE_UNTIL_FOREVER;

        esLocation.availabilityTime(
                availabilityStart
                        .toDateTime(DateTimeZones.UTC)
                        .toDate()
        );
        esLocation.availabilityEndTime(
                availabilityEnd
                        .toDateTime(DateTimeZones.UTC)
                        .toDate()
        );

        esLocation.aliases(location.getAliases());

        return esLocation;
    }

    private Collection<EsLocation> makeESLocations(Content content) {
        return content.getManifestedAs()
                .stream()
                .flatMap(encoding -> encoding.getAvailableAt().stream())
                .filter(location -> location.getPolicy() != null)
                .map(this::toEsLocation)
                .collect(Collectors.toList());
    }

    private Collection<EsTopicMapping> makeESTopics(Content content) {
        Collection<EsTopicMapping> esTopics = new LinkedList<>();
        for (Tag tag : content.getTags()) {
            log.debug("Indexing content {} with tag {}", content.getId(), tag.getTopic());
            esTopics.add(new EsTopicMapping()
                    .topicId(tag.getTopic().longValue())
                    .supervised(tag.isSupervised())
                    .weighting(tag.getWeighting())
                    .relationship(tag.getRelationship()));
        }
        return esTopics;
    }

    private String flattenedOrNull(String string) {
        return string != null ? org.atlasapi.util.Strings.flatten(string) : null;
    }

    private Id toCanonicalId(Id id) throws IndexException {
        try {
            ListenableFuture<ImmutableMap<Long, Long>> result = equivIdIndex.lookup(ImmutableList.of(
                    id.longValue()));
            ImmutableMap<Long, Long> idToCanonical = Futures.get(result, IOException.class);
            if (idToCanonical.get(Long.valueOf(id.longValue())) != null) {
                return Id.valueOf(idToCanonical.get(id.longValue()));
            }
            log.warn("Found no canonical ID for {} using {}", id, id);
            return id;
        } catch (IOException e) {
            throw new IndexException(e);
        }
    }

    public EsContent toEsContainer(Container container) throws IndexException {
        Optional<Map<String, Object>> maybeExisting = getContainer(container.toRef());
        Id canonicalId = toCanonicalId(container.getId());
        EsContent indexed = new EsContent()
                .id(container.getId().longValue())
                .type(ContentType.fromContent(container).get())
                .source(container.getSource() != null ? container.getSource().key() : null)
                .aliases(container.getAliases())
                .title(container.getTitle())
                .genre(container.getGenres())
                .age(null)
                .price(makeEsPrices(container.getManifestedAs()))
                .flattenedTitle(flattenedOrNull(container.getTitle()))
                .parentTitle(container.getTitle())
                .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
                .specialization(container.getSpecialization() != null ?
                                container.getSpecialization().name() :
                                null)
                .priority(container.getPriority() != null
                          ? container.getPriority().getPriority()
                          : null);

        if (maybeExisting.isPresent()) {
            setExistingData(container, indexed, maybeExisting.get());
        } else {
            indexed.locations(makeESLocations(container));
        }

        indexed.topics(makeESTopics(container));
        indexed.sortKey(container.getSortKey());
        indexed.canonicalId(canonicalId.longValue());

        indexed.hasChildren(Boolean.FALSE);
        if (!container.getItemRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
        }

        if (container instanceof Series) {
            Series series = (Series) container;
            indexed.seriesNumber(series.getSeriesNumber());
            if (series.getBrandRef() != null) {
                indexed.brandId(series.getBrandRef().getId());
            }

        }
        return indexed;
    }

    private void setExistingData(Container container, EsContent indexed,
            Map<String, Object> existing) {
        List<Map<String, Object>> locations = dedupeAndMergeLocations(
                container, (List) existing.get(EsContent.LOCATIONS)
        );
        indexed.locations(Iterables.transform(locations, EsLocation::fromMap));
        if (existing.get(EsContent.BROADCASTS) != null) {
            List<Map<String, Object>> broadcasts = (List) existing.get(EsContent.BROADCASTS);
            indexed.broadcasts(Iterables.transform(broadcasts, EsBroadcast::fromMap));
        }
        if (existing.get(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS) != null) {
            indexed.broadcastStartTimeInMillis((List) existing.get(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS));
        }
    }

    private String getDocId(Content content) {
        return String.valueOf(content.getId());
    }

    private String getDocId(ContainerRef container) {
        return String.valueOf(container.getId());
    }

    public void setParentFields(EsContent child, ContainerRef parent) {
        Optional<Map<String, Object>> maybeParent = getContainer(parent);
        if (maybeParent.isPresent()) {
            Map<String, Object> parentContainerData = maybeParent.get();
            Object title = parentContainerData.get(EsContent.TITLE);
            child.parentTitle(title != null ? title.toString() : null);
            Object flatTitle = parentContainerData.get(EsContent.FLATTENED_TITLE);
            child.parentFlattenedTitle(flatTitle != null ? flatTitle.toString() : null);
        }
    }

    public Map<String, Object> denormalizeEpisodeOntoSeries(Episode episode,
            Map<String, Object> series) {
        series.put(
                EsContent.BROADCASTS,
                dedupeAndMergeBroadcasts(
                        episode, (List<Map<String, Object>>) series.get(EsContent.BROADCASTS)
                )
        );
        series.put(
                EsContent.LOCATIONS,
                dedupeAndMergeLocations(
                        episode, (List<Map<String, Object>>) series.get(EsContent.LOCATIONS)
                )
        );
        series.put(
                EsBroadcast.TRANSMISSION_TIME_IN_MILLIS,
                dedupeAndMergeTransmissionTime(
                        episode, (List<Integer>) series.get(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS)
                )
        );

        return series;
    }

    private Object dedupeAndMergeTransmissionTime(Item episode, List<Integer> list) {
        ImmutableList<Long> millis = episode.getBroadcasts().stream()
                .map(b -> b.getTransmissionTime().getMillis())
                .collect(MoreCollectors.toImmutableList());

        if (list == null || list.isEmpty()) {
            return millis;
        }

        return ImmutableList.builder()
                .addAll(millis)
                .addAll(list)
                .build()
                .stream()
                .distinct()
                .collect(MoreCollectors.toImmutableList());
    }

    private List<Map<String, Object>> dedupeAndMergeLocations(Content episode,
            List<Map<String, Object>> existingLocations) {
        ImmutableSet<Location> locations = episode.getManifestedAs().stream()
                .filter(encoding -> encoding != null)
                .flatMap(encoding -> encoding.getAvailableAt().stream())
                .filter(location -> location != null)
                .filter(location -> location.getPolicy() != null)
                .collect(MoreCollectors.toImmutableSet());

        List<Map<String, Object>> nonNullExistingLocations = existingLocations != null
                                                       ? existingLocations
                                                       : ImmutableList.of();
        Predicate<Policy> filter = createLocationNotPresentFilter(
                fromEsLocations(nonNullExistingLocations)
        );

        ImmutableList<Map<String, Object>> newLocations = locations.stream()
                .filter(location -> filter.apply(location.getPolicy()))
                .map(this::toEsLocation)
                .map(EsObject::toMap)
                .collect(MoreCollectors.toImmutableList());

        return ImmutableList.<Map<String, Object>>builder()
                .addAll(nonNullExistingLocations)
                .addAll(newLocations)
                .build();
    }

    private ImmutableList<Policy> fromEsLocations(List<Map<String, Object>> existingLocations) {
        ImmutableList.Builder<Policy> builder = ImmutableList.builder();
        for (Map<String, Object> location : existingLocations) {
            builder.add(fromEsLocation(location));
        }
        return builder.build();
    }

    private Policy fromEsLocation(Map<String, Object> location) {
        Object start = location.get(EsLocation.AVAILABILITY_TIME);
        Object end = location.get(EsLocation.AVAILABILITY_END_TIME);

        Policy pol = new Policy();
        if (start != null) {
            pol.setAvailabilityStart(new DateTime(start).toDateTime(DateTimeZones.UTC));
        }
        if (end != null) {
            pol.setAvailabilityEnd(new DateTime(end).toDateTime(DateTimeZones.UTC));
        }

        return pol;
    }

    private Predicate<Policy> createLocationNotPresentFilter(List<Policy> existingPolicies) {
        return policy -> {
            for (Policy existingPolicy : existingPolicies) {
                if (Objects.equals(
                        toUtc(policy.getAvailabilityStart()),
                        toUtc(existingPolicy.getAvailabilityStart())
                ) &&
                        Objects.equals(
                                toUtc(policy.getAvailabilityEnd()),
                                toUtc(existingPolicy.getAvailabilityEnd())
                        )) {
                    return false;
                }
            }
            return true;
        };
    }

    private ImmutableList<Object> dedupeAndMergeBroadcasts(Item item,
            List<Map<String, Object>> indexedParentBroadcasts) {
        // This will remove from the parent any broadcasts that match those already on the item
        // and then add the actively published item broadcasts back to the parent. This will
        // effectively remove any non actively published ones of this item that were already
        // on the parent

        ImmutableList<Broadcast> itemBroadcasts = item.getBroadcasts().stream()
                .filter(this::broadcastHasNecessaryFields)
                .collect(MoreCollectors.toImmutableList());

        Predicate<Broadcast> broadcastNotPresentFilter =
                createBroadcastNotPresentFilter(itemBroadcasts);

        ImmutableList<Broadcast> parentBroadcasts = fromEsBroadcasts(
                indexedParentBroadcasts != null ? indexedParentBroadcasts : ImmutableList.of()
        )
                .stream()
                .filter(this::broadcastHasNecessaryFields)
                .filter(broadcastNotPresentFilter::apply)
                .collect(MoreCollectors.toImmutableList());

        ImmutableList<Broadcast> activelyPublishedItemBroadcasts = itemBroadcasts.stream()
                .filter(Broadcast::isActivelyPublished)
                .collect(MoreCollectors.toImmutableList());

        return ImmutableList.<Broadcast>builder()
                .addAll(parentBroadcasts)
                .addAll(activelyPublishedItemBroadcasts)
                .build()
                .stream()
                .map(this::toEsBroadcast)
                .map(EsObject::toMap)
                .collect(MoreCollectors.toImmutableList());
    }

    private boolean broadcastHasNecessaryFields(Broadcast broadcast) {
        return broadcast != null
                && broadcast.getTransmissionInterval() != null
                && broadcast.getChannelId() != null;
    }

    private Predicate<Broadcast> createBroadcastNotPresentFilter(
            Collection<Broadcast> existingBroadcasts) {
        return broadcast -> existingBroadcasts.stream()
                .noneMatch(existing -> broadcastIsEqual(broadcast, existing));
    }

    private boolean broadcastIsEqual(Broadcast broadcast, Broadcast existing) {
        return existing.getTransmissionInterval().isEqual(broadcast.getTransmissionInterval())
                && Objects.equals(existing.getChannelId(), broadcast.getChannelId());
    }

    private ImmutableList<Broadcast> fromEsBroadcasts(List<Map<String, Object>> broadcasts) {
        ImmutableList.Builder<Broadcast> builder = ImmutableList.builder();
        for (Map<String, Object> broadcast : broadcasts) {
            builder.add(toBroadcast(broadcast));
        }
        return builder.build();
    }

    private Broadcast toBroadcast(Map<String, Object> broadcast) {
        Object start = broadcast.get(EsBroadcast.TRANSMISSION_TIME);
        Object end = broadcast.get(EsBroadcast.TRANSMISSION_END_TIME);

        DateTime startDate = null;
        DateTime endDate = null;

        if (start != null) {
            startDate = new DateTime(start);
        }
        if (end != null) {
            endDate = new DateTime(end);
        }

        return new Broadcast(
                Id.valueOf((Integer) broadcast.get(EsBroadcast.CHANNEL)),
                startDate,
                endDate,
                true
        );
    }

    private Optional<Map<String, Object>> getContainer(ContainerRef parent) {
        GetRequest request = Requests.getRequest(indexName).id(getDocId(parent));
        GetResponse response = ElasticsearchUtils.getWithTimeout(
                esClient.get(request),
                requestTimeout
        );
        if (response.isExists()) {
            return Optional.of(response.getSource());
        }
        return Optional.empty();
    }

    public EsContent toEsContent(Item item) throws IndexException {
        Id canonical = toCanonicalId(item.getId());
        EsContent esContent = new EsContent()
                .id(item.getId().longValue())
                .type(ContentType.fromContent(item).get())
                .source(item.getSource() != null ? item.getSource().key() : null)
                .aliases(item.getAliases())
                .title(item.getTitle())
                .genre(item.getGenres())
                .age(item.getRestrictions().stream()
                        .map(Restriction::getMinimumAge)
                        .filter(a -> a != null)
                        .max(Integer::compare)
                        .orElse(null)
                )
                .price(makeEsPrices(item.getManifestedAs()))
                .flattenedTitle(flattenedOrNull(item.getTitle()))
                .parentTitle(item.getTitle())
                .parentFlattenedTitle(flattenedOrNull(item.getTitle()))
                .specialization(item.getSpecialization() != null ?
                                item.getSpecialization().name() :
                                null)
                .priority(item.getPriority() != null ? item.getPriority().getPriority() : null)
                .broadcasts(makeESBroadcasts(item))
                .broadcastStartTimeInMillis(itemToBroadcastStartTimes(item))
                .locations(makeESLocations(item))
                .topics(makeESTopics(item))
                .sortKey(item.getSortKey())
                .canonicalId(canonical.longValue());

        if (item.getContainerRef() != null) {
            Id containerId = item.getContainerRef().getId();
            if (ContentType.BRAND.equals(item.getContainerRef().getContentType())) {
                esContent.brandId(containerId);
            }
            if (ContentType.SERIES.equals(item.getContainerRef().getContentType())) {
                esContent.seriesId(containerId);
            }
        }

        if (item instanceof Episode) {
            Episode episode = (Episode) item;
            esContent.episodeNumber(episode.getEpisodeNumber());
            esContent.seriesNumber(episode.getSeriesNumber());
            if (episode.getSeriesRef() != null) {
                esContent.seriesId(episode.getSeriesRef().getId());
            }
        }
        return esContent;
    }

    private Iterable<Long> itemToBroadcastStartTimes(Item item) {
        return item.getBroadcasts().stream()
                .filter(b -> b.getTransmissionTime() != null
                        && Boolean.TRUE.equals(b.isActivelyPublished())
                )
                .map(b -> b.getTransmissionTime().getMillis())
                .collect(MoreCollectors.toImmutableList());
    }
}
