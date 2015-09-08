package org.atlasapi.content;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.util.ElasticsearchUtils;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        return transmissionTime.toDateTime(DateTimeZones.UTC);
    }

    private EsLocation toEsLocation(Policy policy) {
        return new EsLocation()
                .availabilityTime(toUtc(policy.getAvailabilityStart()).toDate())
                .availabilityEndTime(toUtc(policy.getAvailabilityEnd()).toDate());
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
            if (idToCanonical.containsKey(Long.valueOf(id.longValue()))) {
                return Id.valueOf(Long.valueOf(idToCanonical.get(id.longValue())));
            }
            return null;
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
                .priority(container.getPriority() != null ? container.getPriority().getPriority() : null)
                .locations(makeESLocations(container))
                .topics(makeESTopics(container))
                .sortKey(container.getSortKey());
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

    private String getDocId(Content content) {
        return String.valueOf(content.getId());
    }

    private String getDocId(ContainerRef container) {
        return String.valueOf(container.getId());
    }

    public void setParentFields(EsContent child, ContainerRef parent) {
        Optional<Map<String, Object>> maybeParent = getParent(parent);
        if (maybeParent.isPresent()) {
            Map<String, Object> parentContainerData = maybeParent.get();
            Object title = parentContainerData.get(EsContent.TITLE);
            child.parentTitle(title != null ? title.toString() : null);
            Object flatTitle = parentContainerData.get(EsContent.FLATTENED_TITLE);
            child.parentFlattenedTitle(flatTitle != null ? flatTitle.toString() : null);
        }
    }

    private Optional<Map<String, Object>> getParent(ContainerRef parent) {
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
        if (canonical != null)
                esContent.canonicalId(canonical.longValue());

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
