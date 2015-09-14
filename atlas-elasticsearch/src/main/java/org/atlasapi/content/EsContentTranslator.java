package org.atlasapi.content;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.util.ElasticsearchUtils;
import org.atlasapi.util.EsObject;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class EsContentTranslator {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Client esClient;
    private final SecondaryIndex equivIdIndex;
    private final String indexName;
    private final Integer requestTimeout;
    private final ContentResolver contentResolver;

    public EsContentTranslator(
            String indexName,
            Client esClient,
            SecondaryIndex equivIdIndex,
            Long requestTimeout,
            ContentResolver contentResolver
    ) {
        checkArgument(!Strings.isNullOrEmpty(indexName), "Index name cannot be null or empty");
        this.indexName = indexName;
        this.esClient = checkNotNull(esClient);
        this.equivIdIndex = checkNotNull(equivIdIndex);
        this.requestTimeout = checkNotNull(requestTimeout.intValue());
        this.contentResolver = checkNotNull(contentResolver);
    }

    private Collection<EsBroadcast> makeESBroadcasts(Item item) {
        Collection<EsBroadcast> esBroadcasts = new LinkedList<EsBroadcast>();
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
                .map(price -> new EsPriceMapping().currency(price.getCurrency()).value(price.getAmount()))
                .collect(ImmutableCollectors.toList());
    }

    private DateTime toUtc(DateTime transmissionTime) {
        if (transmissionTime == null) {
            return null;
        }
        return transmissionTime.toDateTime(DateTimeZones.UTC);
    }

    private EsLocation toEsLocation(Policy policy) {
        EsLocation location = new EsLocation();
        if (policy.getAvailabilityStart() != null) {
            location.availabilityTime(toUtc(policy.getAvailabilityStart()).toDate());
        }
        if (policy.getAvailabilityEnd() != null) {
            location.availabilityEndTime(toUtc(policy.getAvailabilityEnd()).toDate());
        }
        return location;
    }

    private Collection<EsLocation> makeESLocations(Content content) {
        Collection<EsLocation> esLocations = new LinkedList<>();
        for (Encoding encoding : content.getManifestedAs()) {
            for (Location location : encoding.getAvailableAt()) {
                if (location.getPolicy() != null
                        && location.getPolicy().getAvailabilityStart() != null
                        && location.getPolicy().getAvailabilityEnd() != null) {
                    esLocations.add(toEsLocation(location.getPolicy()));
                }
            }
        }
        return esLocations;
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
            ListenableFuture<ImmutableMap<Long, Long>> result = equivIdIndex.lookup(ImmutableList.of(id.longValue()));
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

    private Integer ageRestrictionFromContainer(Container container) {
        // TODO fix this, number of item refs in containers is too high to resolve without C* timeouts
        if (true) {
            return null;
        }

        try {
            if (container.getItemRefs() == null || container.getItemRefs().isEmpty()) {
                return null;
            }

            Resolved<Content> resolved = Futures.get(
                    contentResolver.resolveIds(Iterables.transform(container.getItemRefs(),
                            ResourceRef::getId)),
                    IOException.class
            );

            return ImmutableList.copyOf(resolved.getResources()).stream()
                    .filter(i -> i instanceof Item)
                    .map(i -> (Item) i)
                    .filter(i -> (i.getRestrictions() != null) || !i.getRestrictions().isEmpty())
                    .flatMap(i -> i.getRestrictions().stream())
                    .map(Restriction::getMinimumAge)
                    .filter(a -> a != null)
                    .max(Integer::compare)
                    .orElse(null);
        } catch (IOException e) {
            throw Throwables.propagate(e);
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
                .age(ageRestrictionFromContainer(container))
                .price(makeEsPrices(container.getManifestedAs()))
                .flattenedTitle(flattenedOrNull(container.getTitle()))
                .parentTitle(container.getTitle())
                .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
                .specialization(container.getSpecialization() != null ?
                        container.getSpecialization().name() :
                        null)
                .priority(container.getPriority() != null ? container.getPriority().getPriority() : null);

        if (maybeExisting.isPresent()) {
            setExistingData(container, indexed, maybeExisting.get());
        } else {
            indexed.locations(makeESLocations(container));
        }

        indexed.topics(makeESTopics(container));
        indexed.sortKey(container.getSortKey());
        if (canonicalId != null) {
            indexed.canonicalId(canonicalId.longValue());
        }

        indexed.hasChildren(Boolean.FALSE);
        if (!container.getItemRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
        }

        if (container instanceof Series) {
            Series series = (Series) container;
            indexed.seriesNumber(series.getSeriesNumber());
            indexed.seriesId(container.getId());
            if (series.getBrandRef() != null) {
                indexed.brandId(series.getBrandRef().getId());
            }

        }
        return indexed;
    }

    private void setExistingData(Container container, EsContent indexed, Map<String, Object> existing) {
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

    public void denormalizeEpisodeOntoSeries(Item content) {
        if (content instanceof Episode) {
            Episode episode = (Episode) content;
            if (episode.getSeriesRef() != null) {
                SeriesRef seriesRef = episode.getSeriesRef();
                Optional<Map<String, Object>> maybeParent = getContainer(seriesRef);
                if (maybeParent.isPresent()) {
                    Map<String, Object> series = maybeParent.get();
                    series.put(
                            EsContent.BROADCASTS,
                            dedupeAndMergeBroadcasts(episode, (List) series.get(EsContent.BROADCASTS))
                    );
                    series.put(
                            EsContent.LOCATIONS,
                            dedupeAndMergeLocations(episode, (List) series.get(EsContent.LOCATIONS))
                    );
                    series.put(
                            EsBroadcast.TRANSMISSION_TIME_IN_MILLIS,
                            dedupeAndMergeTransmissionTime(episode, (List) series.get(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS))
                    );
                    reindexParent(seriesRef, series);
                }
            }
        }
    }

    private Object dedupeAndMergeTransmissionTime(Item episode, List<Integer> list) {
        ImmutableList<Long> millis = episode.getBroadcasts().stream()
                .map(b -> b.getTransmissionTime().getMillis())
                .collect(ImmutableCollectors.toList());

        if (list == null || list.isEmpty()) {
            return millis;
        }

        return ImmutableList.builder()
                .addAll(millis)
                .addAll(list)
                .build()
                .stream()
                .distinct()
                .collect(ImmutableCollectors.toList());
    }

    private List<Map<String, Object>> dedupeAndMergeLocations(Content episode, List<Map<String, Object>> existingLocations) {
        ImmutableSet<Policy> policies = episode.getManifestedAs().stream()
                .filter(encoding -> encoding != null)
                .flatMap(encoding -> encoding.getAvailableAt().stream())
                .filter(location -> location != null)
                .map(Location::getPolicy)
                .filter(policy -> policy != null)
                .collect(ImmutableCollectors.toSet());

        Predicate<Policy> filter = createLocationNotPresentFilter(
                fromEsLocations(
                        existingLocations != null ? existingLocations : ImmutableList.of()
                )
        );

        ImmutableList<Map<String, Object>> newPolicies = policies.stream()
                .filter(filter::apply)
                .map(this::toEsLocation)
                .map(EsObject::toMap)
                .collect(ImmutableCollectors.toList());

        return (List) ImmutableList.builder().addAll(existingLocations).addAll(newPolicies).build();
    }

    private ImmutableList<Policy> fromEsLocations(List<Map<String, Object>> existingLocations) {
        ImmutableList.Builder<Policy> builder = ImmutableList.builder();
        for (Map<String, Object> location : existingLocations) {
            builder.add(fromEsLocation(location));
        }
        return builder.build();
    }

    private Policy fromEsLocation(Map<String, Object> location) {
        String start = (String) location.get(EsLocation.AVAILABILITY_TIME);
        String end = (String) location.get(EsLocation.AVAILABILITY_END_TIME);
        DateTime startDt = null;
        DateTime endDt = null;
        if (!Strings.isNullOrEmpty(start)) {
            startDt = toUtc(DateTime.parse(start));
        }
        if (!Strings.isNullOrEmpty(end)) {
            endDt = toUtc(DateTime.parse(end));
        }
        Policy pol = new Policy();
        if (endDt != null) {
            pol.setAvailabilityStart(startDt);
        }
        if (startDt != null) {
            pol.setAvailabilityEnd(endDt);
        }
        return pol;
    }

    private Predicate<Policy> createLocationNotPresentFilter(List<Policy> existingPolicies) {
        return policy -> {
            for (Policy existingPolicy : existingPolicies) {
                if (Objects.equals(toUtc(policy.getAvailabilityStart()), toUtc(existingPolicy.getAvailabilityStart())) &&
                        Objects.equals(toUtc(policy.getAvailabilityEnd()), toUtc(existingPolicy.getAvailabilityEnd()))) {
                    return false;
                }
            }
            return true;
        };
    }

    private void reindexParent(SeriesRef seriesRef, Map<String, Object> series) {
        IndexRequest req = Requests.indexRequest(indexName)
                .type(EsContent.TOP_LEVEL_CONTAINER)
                .id(getDocId(seriesRef))
                .source(series);
        try {
            esClient.index(req).get();
        } catch (Exception e) {
            log.error("Failed to update Series {} with Episode data", seriesRef.getId(), e);
        }
    }

    private ImmutableList<Object> dedupeAndMergeBroadcasts(Item item, List<Map<String, Object>> indexedBroadcasts) {
        List<Broadcast> parentIndexedBroadcasts = fromEsBroadcasts(
                indexedBroadcasts != null ? indexedBroadcasts : ImmutableList.of()
        );

        Predicate<Broadcast> broadcastNotPresentFilter =
                createBroadcastNotPresentFilter(parentIndexedBroadcasts);

        ImmutableList<Map<String, Object>> newBroadcasts = item.getBroadcasts().stream()
                .filter(broadcastNotPresentFilter::apply)
                .map(this::toEsBroadcast)
                .map(EsObject::toMap)
                .collect(ImmutableCollectors.toList());

        ImmutableList.Builder<Object> merged = ImmutableList.builder().addAll(newBroadcasts);
        if (indexedBroadcasts != null) {
            merged.addAll(indexedBroadcasts);
        }
        return merged.build();
    }

    private Predicate<Broadcast> createBroadcastNotPresentFilter(Iterable<Broadcast> existingBroadcasts) {
        return broadcast -> {
            if (broadcast == null || broadcast.getTransmissionInterval() == null) {
                return false;
            }
            for (Broadcast existingBroadcast : existingBroadcasts) {
                if (existingBroadcast.getTransmissionInterval()
                        .isEqual(broadcast.getTransmissionInterval())) {
                    return false;
                }
            }
            return true;
        };
    }

    private List<Broadcast> fromEsBroadcasts(List<Map<String, Object>> broadcasts) {
        ImmutableList.Builder<Broadcast> builder = ImmutableList.builder();
        for (Map<String, Object> broadcast : broadcasts) {
            builder.add(toBroadcast(broadcast));
        }
        return builder.build();
    }

    private Broadcast toBroadcast(Map<String, Object> broadcast) {
        String start = (String) broadcast.get(EsBroadcast.TRANSMISSION_TIME);
        String end = (String) broadcast.get(EsBroadcast.TRANSMISSION_END_TIME);
        DateTime startDt = null;
        DateTime endDt = null;
        if (!Strings.isNullOrEmpty(start)) {
            startDt = DateTime.parse(start);
        }
        if (!Strings.isNullOrEmpty(end)) {
            endDt = DateTime.parse(end);
        }
        return new Broadcast(
                Id.valueOf((Integer) broadcast.get(EsBroadcast.CHANNEL)),
                startDt,
                endDt,
                true
        );
    }

    private Optional<Map<String, Object>> getContainer(ContainerRef parent) {
        GetRequest request = Requests.getRequest(indexName).id(getDocId(parent));
        GetResponse response = ElasticsearchUtils.getWithTimeout(esClient.get(request), requestTimeout);
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
                .sortKey(item.getSortKey());
        if (canonical != null) {
            esContent.canonicalId(canonical.longValue());
        }

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
                .filter(b -> b.getTransmissionTime() != null)
                .map(b -> b.getTransmissionTime().getMillis())
                .collect(ImmutableCollectors.toList());
    }
}
