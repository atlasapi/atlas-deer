package org.atlasapi.content;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.EsPersistenceException;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.EsSortQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.EsSchema.CONTENT_INDEX;

public class EsContentIndex extends AbstractIdleService implements ContentIndex {

    public static final String CONTENT = "content";
    private final Logger log = LoggerFactory.getLogger(EsContentIndex.class);

    private static final int DEFAULT_LIMIT = 50;

    private final Node esClient;
    private final String index;
    private final long requestTimeout;

    private Set<String> existingIndexes;
    private final ContentResolver resolver;

    private final EsQueryBuilder queryBuilder = new EsQueryBuilder();
    private final EsSortQueryBuilder sortBuilder = new EsSortQueryBuilder();
    private final ContentGroupIndexUpdater contentGroupIndexUpdater = new ContentGroupIndexUpdater();

    public EsContentIndex(Node esClient, String indexName, long requestTimeout, ContentResolver resolver) {
        this.esClient = checkNotNull(esClient);
        this.requestTimeout = requestTimeout;
        this.index = checkNotNull(indexName);
        this.resolver = checkNotNull(resolver);
    }

    @Override
    protected void startUp() throws IOException {
        if (createIndex(index)) {
            putTypeMappings();
        }
        log.info("Found existing indices {}", existingIndexes);
    }

    @Override
    protected void shutDown() throws Exception {

    }
    private boolean createIndex(String name) {
        ActionFuture<IndicesExistsResponse> exists = esClient.client().admin().indices().exists(
            Requests.indicesExistsRequest(name)
        );
        if (!timeoutGet(exists).isExists()) {
            try {
                log.info("Creating index {}", name);
                timeoutGet(esClient.client().admin().indices().create(Requests.createIndexRequest(
                        name)));
            } catch (IndexAlreadyExistsException iaee) {
                log.info("Already exists: {}", name);
                return false;
            }
            return true;
        } else {
            log.info("Index {} exists", name);
            return false;
        }
    }

    private void putTypeMappings() throws IOException {
        log.info("Putting mapping for type {}", EsContent.TOP_LEVEL_CONTAINER);
        doMappingRequest(Requests.putMappingRequest(index)
            .type(EsContent.TOP_LEVEL_CONTAINER)
            .source(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_CONTAINER)));
        log.info("Putting mapping for type {}", EsContent.TOP_LEVEL_ITEM);
        doMappingRequest(Requests.putMappingRequest(index)
            .type(EsContent.TOP_LEVEL_ITEM)
            .source(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_ITEM)));
        log.info("Putting mapping for type {}", EsContent.CHILD_ITEM);
        doMappingRequest(Requests.putMappingRequest(index)
                .type(EsContent.CHILD_ITEM)
                .source(EsContent.getChildMapping()));
    }

    private void doMappingRequest(PutMappingRequest req) {
        try {
            timeoutGet(esClient.client().admin().indices().putMapping(req));
        } catch (MergeMappingException mme) {
            log.info("Merge Mapping Exception: {}/{}", req.indices(), req.type());
        }

    }

    private EsContent toEsContent(Item item) {
        return new EsContent()
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
                .broadcasts(makeESBroadcasts(item))
                .locations(makeESLocations(item))
                .topics(makeESTopics(item));
    }

    private EsContent toEsContainer(Container container) {
        EsContent indexed = new EsContent()
            .id(container.getId().longValue())
            .type(ContentType.fromContent(container).get())
            .source(container.getSource() != null ? container.getSource().key() : null)
            .aliases(container.getAliases())
            .title(container.getTitle())
            .genre(container.getGenres())
            .age(ageRestrictionFromContainer(container))
            .flattenedTitle(flattenedOrNull(container.getTitle()))
            .parentTitle(container.getTitle())
            .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
            .specialization(container.getSpecialization() != null ?
                    container.getSpecialization().name() :
                    null)
            .topics(makeESTopics(container));
        if (!container.getItemRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
            indexChildrenData(container);
        } else {
            indexed.hasChildren(Boolean.FALSE);
        }
        return indexed;
    }

    private String flattenedOrNull(String string) {
        return string != null ? Strings.flatten(string) : null;
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
                .transmissionTimeInMillis(toUtc(broadcast.getTransmissionTime()).getMillis())
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

    private Collection<EsLocation> makeESLocations(Item item) {
        Collection<EsLocation> esLocations = new LinkedList<EsLocation>();
        for (Encoding encoding : item.getManifestedAs()) {
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
        Collection<EsTopicMapping> esTopics = new LinkedList<EsTopicMapping>();
        for (TopicRef topic : content.getTopicRefs()) {
            esTopics.add(new EsTopicMapping()
                    .topicId(topic.getTopic().longValue())
                    .supervised(topic.isSupervised())
                    .weighting(topic.getWeighting())
                    .relationship(topic.getRelationship()));
        }
        return esTopics;
    }

    private void indexContainer(Container container) {
        EsContent indexed = toEsContainer(container);
        IndexRequest request = Requests.indexRequest(CONTENT_INDEX)
            .type(EsContent.TOP_LEVEL_CONTAINER)
            .id(getDocId(container))
            .source(indexed.toMap());
        timeoutGet(esClient.client().index(request));
        log.debug("indexed {}", container);
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
                    resolver.resolveIds(Iterables.transform(container.getItemRefs(),
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




    private void fillParentData(EsContent child, ContainerRef parent) {
        Map<String, Object> indexedContainer = trySearchParent(parent);
        if (indexedContainer != null) {
            Object title = indexedContainer.get(EsContent.TITLE);
            child.parentTitle(title != null ? title.toString() : null);
            Object flatTitle = indexedContainer.get(EsContent.FLATTENED_TITLE);
            child.parentFlattenedTitle(flatTitle != null ? flatTitle.toString() : null);
        }
    }

    private void indexChildrenData(Container parent) {
        BulkRequest bulk = Requests.bulkRequest();
        for (ItemRef child : parent.getItemRefs()) {
            Map<String, Object> indexedChild = trySearchChild(parent, child);
            if (indexedChild != null) {
                if (parent.getTitle() != null) {
                    indexedChild.put(EsContent.PARENT_TITLE, parent.getTitle());
                    indexedChild.put(EsContent.PARENT_FLATTENED_TITLE, Strings.flatten(parent.getTitle()));
                    bulk.add(Requests.indexRequest(CONTENT_INDEX).
                            type(EsContent.CHILD_ITEM).
                            parent(getDocId(parent)).
                            id(getDocId(child)).
                            source(indexedChild));
                }
            }
        }
        if (bulk.numberOfActions() > 0) {
            BulkResponse response = timeoutGet(esClient.client().bulk(bulk));
            if (response.hasFailures()) {
                throw new EsPersistenceException("Failed to index children for container: " + getDocId(parent));
            }
        }
    }

    private String getDocId(ItemRef child) {
        return String.valueOf(child.getId());
    }

    private String getDocId(Content content) {
        return String.valueOf(content.getId());
    }

    private String getDocId(ContainerRef container) {
        return String.valueOf(container.getId());
    }

    private Map<String, Object> trySearchParent(ContainerRef parent) {
        GetRequest request = Requests.getRequest(CONTENT_INDEX).id(getDocId(parent));
        GetResponse response = timeoutGet(esClient.client().get(request));
        if (response.isExists()) {
            return response.getSource();
        } else {
            return null;
        }
    }

    private Map<String, Object> trySearchChild(Container parent, ItemRef child) {
        try {
            GetRequest request = Requests.getRequest(CONTENT_INDEX)
                    .parent(getDocId(parent))
                    .id(getDocId(child));
            GetResponse response = timeoutGet(esClient.client().get(request));
            if (response.isExists()) {
                return response.getSource();
            } else {
                return null;
            }
        } catch (NoShardAvailableActionException ex) {
            return null;
        }
    }

    private <T> T timeoutGet(ActionFuture<T> future) {
        try {
            return future.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (ElasticsearchException ese) {
            Throwable root = Throwables.getRootCause(ese);
            Throwables.propagateIfInstanceOf(root, ElasticsearchException.class);
            throw Throwables.propagate(ese);
        }
    }

    @Override
    public void index(Content content) throws IndexException {
        /* We unindex content before inserting any new records to avoid
            duplicates when a piece of content changes type (e.g. from top level to child)
         */
        unindexContent(content);
        if (!content.isActivelyPublished()) {
            return;
        }
        try {
            content.accept(new ContentVisitorAdapter<Void>() {

                @Override
                protected Void visitItem(Item item) {
                    indexItem(item);
                    return null;
                }

                @Override
                protected Void visitContainer(Container container) {
                    indexContainer(container);
                    return null;
                }
            });
        } catch (RuntimeIndexException rie) {
            throw new IndexException(rie.getMessage(), rie.getCause());
        }
    }

    @Override
    public void index(ContentGroup cg) throws IndexException {
        log.debug("Indexing {}", cg);
        contentGroupIndexUpdater.index(cg);
    }

    private void unindexContent(Content content) {
        Long id = content.getId().longValue();
        log.debug("Content {} is not actively published, removing from index", id);
        deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_CONTAINER);
        deleteFromIndexIfExists(id, EsContent.CHILD_ITEM);
        deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_ITEM);
    }

    private void deleteFromIndexIfExists(Long id, String mappingType) {
        try {
            boolean exists = new ExistsRequestBuilder(esClient.client())
                    .setTypes(mappingType)
                    .setQuery(QueryBuilders.termQuery(EsContent.ID, id.toString()))
                    .execute()
                    .get()
                    .exists();

            if (exists) {
                esClient.client().delete(new DeleteRequest(CONTENT, mappingType, id.toString())).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete content {} due to {}", id, e.toString());
        }
    }

    private class RuntimeIndexException extends RuntimeException {

        public RuntimeIndexException(String message, Throwable cause) {
            super(message, cause);
        }

    }

    @Override
    public ListenableFuture<FluentIterable<Id>> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection, Optional<QueryOrdering> maybeOrder) {
        SettableFuture<SearchResponse> response = SettableFuture.create();

        SearchRequestBuilder reqBuilder = esClient.client()
                .prepareSearch(index)
                .setTypes(EsContent.CHILD_ITEM, EsContent.TOP_LEVEL_CONTAINER, EsContent.TOP_LEVEL_ITEM)
                .setQuery(queryBuilder.buildQuery(query))
                .addField(EsContent.ID)
                .setPostFilter(FiltersBuilder.buildForPublishers(EsContent.SOURCE, publishers))
                .setFrom(selection.getOffset())
                .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT));

        if (maybeOrder.isPresent()) {
            QueryOrdering order = maybeOrder.get();
            reqBuilder.addSort(
                    SortBuilders
                            .fieldSort(order.getPath())
                            .order(order.isAscending() ? SortOrder.ASC : SortOrder.DESC)
            );
        }

        reqBuilder.execute(FutureSettingActionListener.setting(response));

        return Futures.transform(response, (SearchResponse input) -> {
            /* TODO
             * if selection.offset + selection.limit < totalHits
             * then we have more: return for use with response.
             */
            return FluentIterable.from(input.getHits()).transform(hit -> {
                Long id = hit.field(EsContent.ID).<Number>value().longValue();
                return Id.valueOf(id);
            });
        });
    }

    private void indexItem(Item item) {
        try {
            EsContent esContent = toEsContent(item);

            BulkRequest requests = Requests.bulkRequest();
            IndexRequest mainIndexRequest;
            ContainerRef container = item.getContainerRef();
            if (container != null) {
                fillParentData(esContent, container);
                mainIndexRequest = Requests.indexRequest(CONTENT_INDEX)
                        .type(EsContent.CHILD_ITEM)
                        .id(getDocId(item))
                        .source(esContent.toMap())
                        .parent(getDocId(container));
            } else {
                mainIndexRequest = Requests.indexRequest(CONTENT_INDEX)
                        .type(EsContent.TOP_LEVEL_ITEM)
                        .id(getDocId(item))
                        .source(esContent.hasChildren(false).toMap());
            }

            requests.add(mainIndexRequest);
            BulkResponse resp = timeoutGet(esClient.client().bulk(requests));
            log.debug("indexed {} ({}ms)", item, resp.getTookInMillis());
        } catch (Exception e) {
            throw new RuntimeIndexException("Error indexing " + item, e);
        }
    }

    public class ContentGroupIndexUpdater {

        public void index(ContentGroup group) throws IndexException {
            try {
                ImmutableSet<Id> contentToBeInGroup = groupToChildIds(group);
                ImmutableSet<SearchHit> contentCurrentlyInGroup = findContentByContentGroup(group.getId());
                ImmutableSet<SearchHit> contentToRemoveGroupFrom =
                        findContentNoLongerInGroup(contentToBeInGroup, contentCurrentlyInGroup);

                for (SearchHit hit : contentToRemoveGroupFrom) {
                    removeGroupFromContentIndexEntry(hit, group.getId());
                }

                ImmutableSet<SearchHitField> contentCurrentlyInGroupIds =
                        ImmutableSet.copyOf(Iterables.transform(contentCurrentlyInGroup, hit -> hit.field(EsContent.ID)));


                ImmutableSet<Id> contentToAddGroupTo =
                        Sets.difference(contentToBeInGroup, contentCurrentlyInGroupIds).immutableCopy();
                addGroupToContent(contentToAddGroupTo, group.getId());
            } catch (Exception e) {
                throw new IndexException(e);
            }
        }

        private void addGroupToContent(Iterable<Id> contentIds, Id groupId) throws IOException {
            Resolved<Content> resolved = Futures.get(
                    resolver.resolveIds(contentIds),
                    IOException.class
            );
            for (Content content : resolved.getResources().toList()) {
                log.info("Adding {} to content group {}", content.getId(), groupId);
                String mapping = typeMappingFor(content);
                appendContentGroupToIndexEntryFor(content.getId(), groupId, mapping);
            }
        }

        private String typeMappingFor(Content content) {
            if (content instanceof Brand) {
                return EsContent.TOP_LEVEL_CONTAINER;
            }
            if (content instanceof Series) {
                Series series = (Series) content;
                if (series.getBrandRef() == null) {
                    return EsContent.TOP_LEVEL_CONTAINER;
                } else {
                    return EsContent.CHILD_ITEM;
                }
            }
            if (content instanceof Item) {
                Item item = (Item) content;
                if (item.getContainerRef() == null) {
                    return EsContent.TOP_LEVEL_ITEM;
                } else {
                    return EsContent.CHILD_ITEM;
                }
            }
            return EsContent.TOP_LEVEL_ITEM;
        }

        private ImmutableSet<SearchHit> findContentNoLongerInGroup(
                Set<Id> contentToBeInGroup, Set<SearchHit> contentCurrentlyInGroup) {

            return contentCurrentlyInGroup.stream()
                    .filter(hit -> !contentToBeInGroup.contains(Id.valueOf(hit.field(EsContent.CONTENT_GROUPS).<String>getValue())))
                    .collect(ImmutableCollectors.toSet());

        }

        /* Removes the content group id provided from the index entry represented by the SearchHit */
        private void removeGroupFromContentIndexEntry(SearchHit contentHit, Id groupId) throws ExecutionException, InterruptedException {
            ImmutableList<String> newContentGroupsValue =
                    removeGroupFromGroupList(contentHit, groupId);

            Id contentId = Id.valueOf(contentHit.field(EsContent.ID).<Integer>getValue());
            String typeMapping = contentHit.getType();

            UpdateRequestBuilder reqBuilder =
                    new UpdateRequestBuilder(esClient.client(), CONTENT, typeMapping, contentId.toString());
            UpdateResponse result = reqBuilder.setDoc(EsContent.CONTENT_GROUPS, newContentGroupsValue)
                    .setFields(EsContent.CONTENT_GROUPS)
                    .execute()
                    .get();
        }

        /* Add the content group id provided to the index entry represented by the SearchHit */
        private void appendContentGroupToIndexEntryFor(Id contentId, Id groupId, String typeMapping) {
            GetResponse resp = new GetRequestBuilder(esClient.client(), CONTENT)
                    .setId(contentId.toString())
                    .setFields(EsContent.CONTENT_GROUPS)
                    .get();

            ImmutableList.Builder<Object> idList = ImmutableList.builder();
            idList.add(groupId.toBigInteger());

            Map<String, GetField> fields = resp.getFields();

            if (fields != null && fields.containsKey(EsContent.CONTENT_GROUPS)) {
                idList.addAll(fields.get(EsContent.CONTENT_GROUPS).getValues());
            }

            UpdateRequestBuilder reqBuilder =
                    new UpdateRequestBuilder(esClient.client(), CONTENT, typeMapping, contentId.toString());
            reqBuilder.setDoc(EsContent.CONTENT_GROUPS, idList.build())
                    .get();
        }

        /* Returns a list of content group ids taken from the SearchHit and having removed the specified Id */
        private ImmutableList<String> removeGroupFromGroupList(SearchHit contentHit, Id groupId) {
            return contentHit.field(EsContent.CONTENT_GROUPS).getValues().stream()
                    .map(cgId -> Id.valueOf(String.valueOf(cgId)))
                    .filter(cgId -> !groupId.equals(cgId))
                    .map(Id::toString)
                    .collect(ImmutableCollectors.toList());
        }

        private ImmutableSet<Id> groupToChildIds(ContentGroup group) {
            return group.getContents()
                    .stream()
                    .map(ResourceRef::getId)
                    .collect(ImmutableCollectors.toSet());
        }

        private ImmutableSet<SearchHit> findContentByContentGroup(Id id) throws IndexException {
            SettableFuture<SearchResponse> result = SettableFuture.create();
            TermFilterBuilder idFilter = FilterBuilders.termFilter(
                    EsContent.CONTENT_GROUPS,
                    id.toBigInteger().toString()
            );

            SearchRequestBuilder reqBuilder = new SearchRequestBuilder(esClient.client())
                    .addFields(EsContent.ID, EsContent.CONTENT_GROUPS, EsContent.TYPE)
                    .setPostFilter(idFilter);
            reqBuilder.execute(FutureSettingActionListener.setting(result));

            SearchHits hits = Futures.get(result, IndexException.class)
                    .getHits();

            return ImmutableSet.copyOf(hits.getHits());
        }
    }
}