package org.atlasapi.content;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.EsTopic;
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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsContentIndex extends AbstractIdleService implements ContentIndex {

    private final Logger log = LoggerFactory.getLogger(EsContentIndex.class);

    private static final int DEFAULT_LIMIT = 50;

    private final Node esClient;
    private final String index;
    private final long requestTimeout;
    private final ChannelGroupResolver channelGroupResolver;

    private Set<String> existingIndexes;
    private final ContentResolver resolver;

    private final EsQueryBuilder queryBuilder = new EsQueryBuilder();
    private final EsSortQueryBuilder sortBuilder = new EsSortQueryBuilder();

    public EsContentIndex(Node esClient, String indexName, long requestTimeout, ContentResolver resolver, ChannelGroupResolver channelGroupResolver) {
        this.esClient = checkNotNull(esClient);
        this.requestTimeout = requestTimeout;
        this.index = checkNotNull(indexName);
        this.resolver = checkNotNull(resolver);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
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
                .priority(item.getPriority() != null ? item.getPriority().getPriority() : null)
                .broadcasts(makeESBroadcasts(item))
                .broadcastStartTimeInMillis(itemToBroadcastStartTimes(item))
                .locations(makeESLocations(item))
                .topics(makeESTopics(item));
    }

    private Iterable<Long> itemToBroadcastStartTimes(Item item) {
        return item.getBroadcasts().stream()
                .filter(b -> b.getTransmissionTime() != null)
                .map(b -> b.getTransmissionTime().getMillis())
                .collect(ImmutableCollectors.toList());
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
            .price(makeEsPrices(container.getManifestedAs()))
            .flattenedTitle(flattenedOrNull(container.getTitle()))
            .parentTitle(container.getTitle())
            .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
            .specialization(container.getSpecialization() != null ?
                    container.getSpecialization().name() :
                    null)
            .priority(container.getPriority() != null ? container.getPriority().getPriority() : null)
            .locations(makeESLocations(container))
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
        Collection<EsLocation> esLocations = new LinkedList<EsLocation>();
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
        Collection<EsTopicMapping> esTopics = new LinkedList<EsTopicMapping>();
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

    private void indexContainer(Container container) {
        EsContent indexed = toEsContainer(container);
        IndexRequest request = Requests.indexRequest(index)
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
                    bulk.add(Requests.indexRequest(index).
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
                log.error(response.buildFailureMessage());
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
        GetRequest request = Requests.getRequest(index).id(getDocId(parent));
        GetResponse response = timeoutGet(esClient.client().get(request));
        if (response.isExists()) {
            return response.getSource();
        } else {
            return null;
        }
    }

    private Map<String, Object> trySearchChild(Container parent, ItemRef child) {
        try {
            GetRequest request = Requests.getRequest(index)
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
                esClient.client().delete(new DeleteRequest(index, mappingType, id.toString())).get();
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
    public ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection, Optional<IndexQueryParams> queryParams) {
        SettableFuture<SearchResponse> response = SettableFuture.create();

        QueryBuilder queryBuilder = this.queryBuilder.buildQuery(query);


        SearchRequestBuilder reqBuilder = esClient.client()
                .prepareSearch(index)
                .setTypes(EsContent.CHILD_ITEM, EsContent.TOP_LEVEL_CONTAINER, EsContent.TOP_LEVEL_ITEM)
                .addField(EsContent.ID)
                .setPostFilter(FiltersBuilder.buildForPublishers(EsContent.SOURCE, publishers))
                .setFrom(selection.getOffset())
                .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT));

        if (queryParams.isPresent()) {
            if (queryParams.get().getFuzzyQueryParams().isPresent()) {
                queryBuilder = addTitleQuery(queryParams, queryBuilder);
                reqBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
            }

            if (queryParams.get().getOrdering().isPresent()) {
                addSortOrder(queryParams, reqBuilder);
            }

 //removing temporarily since this is currently broken
   //         if (queryParams.get().getRegionId().isPresent()) {
     //           queryBuilder = addRegionFilter(queryParams, queryBuilder);
       //     }

            if (queryParams.get().getTopicFilterIds().isPresent()) {
                queryBuilder = applyTopicIdFilters(
                        queryParams.get().getTopicFilterIds().get(),
                        queryBuilder
                );
            }
            if (queryParams.get().shouldFilterUnavailableContainers()) {
                queryBuilder = addContainerAvailabilityFilter(queryBuilder);
            }
        }

        if (queryParams.isPresent() && queryParams.get().getBroadcastWeighting().isPresent()) {
            queryBuilder = BroadcastQueryBuilder.build(
                    queryBuilder,
                    queryParams.get().getBroadcastWeighting().get()
            );
        } else {
            queryBuilder = BroadcastQueryBuilder.build(
                    queryBuilder,
                    5f
            );
        }

        reqBuilder.addSort(EsContent.ID, SortOrder.ASC);

        log.debug(queryBuilder.toString());

        reqBuilder.setQuery(queryBuilder);
        reqBuilder.execute(FutureSettingActionListener.setting(response));

        /* TODO
         * if selection.offset + selection.limit < totalHits
         * then we have more: return for use with response.
         */
        return Futures.transform(response, (SearchResponse input) -> {
            return new IndexQueryResult(FluentIterable.from(input.getHits()).transform(hit -> {
                Long id = hit.field(EsContent.ID).<Number>value().longValue();
                return Id.valueOf(id);
            }), input.getHits().getTotalHits());
        });
    }

    private QueryBuilder addContainerAvailabilityFilter(QueryBuilder queryBuilder) {
        return QueryBuilders.hasChildQuery(
                EsContent.CHILD_ITEM,
                QueryBuilders.nestedQuery(
                        EsContent.LOCATIONS,
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.rangeQuery("availabilityTime")
                                        .gte(DateTime.now().toString()))
                                .must(QueryBuilders.rangeQuery("availabilityEndTime")
                                        .lte(DateTime.now().toString()))
                                .must(queryBuilder)
                )
        );
    }

    public QueryBuilder applyTopicIdFilters(List<List<InclusionExclusionId>> topicIdSets, QueryBuilder queryBuilder) {
        ImmutableList.Builder<FilterBuilder> topicIdQueries = ImmutableList.builder();
        for (List<InclusionExclusionId> idSet : topicIdSets) {
            BoolFilterBuilder filterForThisSet = FilterBuilders.boolFilter();
            for (InclusionExclusionId id : idSet) {
                addFilterForTopicId(filterForThisSet, id);
            }
            topicIdQueries.add(filterForThisSet);
        }
        BoolFilterBuilder topicIdBoolFilter = FilterBuilders.boolFilter();
        topicIdQueries.build().forEach(topicIdBoolFilter::should);
        return QueryBuilders.filteredQuery(queryBuilder, topicIdBoolFilter);
    }

    private void addFilterForTopicId(BoolFilterBuilder filterBuilder, InclusionExclusionId id) {
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

    private void addSortOrder(Optional<IndexQueryParams> queryParams, SearchRequestBuilder reqBuilder) {
        QueryOrdering order = queryParams.get().getOrdering().get();
        if ("relevance".equalsIgnoreCase(order.getPath())) {
            reqBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        } else {
            reqBuilder.addSort(
                    SortBuilders
                            .fieldSort(translateOrderField(order.getPath()))
                            .order(order.isAscending() ? SortOrder.ASC : SortOrder.DESC)
            );
        }
    }

    private String translateOrderField(String orderField) {
        if ("title".equalsIgnoreCase(orderField)) {
            return "flattenedTitle";
        }
        return orderField;
    }


    private QueryBuilder addTitleQuery(Optional<IndexQueryParams> queryParams, QueryBuilder queryBuilder) {
        FuzzyQueryParams searchParams = queryParams.get().getFuzzyQueryParams().get();
        queryBuilder = QueryBuilders.boolQuery()
                .must(queryBuilder)
                .must(TitleQueryBuilder.build(searchParams.getSearchTerm(), searchParams.getBoost().orElse(0F)));
        return queryBuilder;
    }

    @SuppressWarnings("unchecked")
    private QueryBuilder addRegionFilter(Optional<IndexQueryParams> queryParams, QueryBuilder queryBuilder) {
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

        return QueryBuilders.filteredQuery(
                queryBuilder,
                FilterBuilders.termsFilter(
                        EsContent.BROADCASTS + "." + EsBroadcast.CHANNEL, channelsIdsForRegion
                )
        );
    }

    private void indexItem(Item item) {
        try {
            EsContent esContent = toEsContent(item);

            BulkRequest requests = Requests.bulkRequest();
            IndexRequest mainIndexRequest;
            ContainerRef container = item.getContainerRef();
            if (container != null) {
                fillParentData(esContent, container);
                mainIndexRequest = Requests.indexRequest(index)
                        .type(EsContent.CHILD_ITEM)
                        .id(getDocId(item))
                        .source(esContent.toMap())
                        .parent(getDocId(container));
            } else {
                mainIndexRequest = Requests.indexRequest(index)
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

}