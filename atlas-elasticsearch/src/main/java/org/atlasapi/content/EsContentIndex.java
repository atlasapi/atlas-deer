package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.EsSchema.CONTENT_INDEX;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.EsPersistenceException;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;
import org.atlasapi.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;

public class EsContentIndex extends AbstractIdleService implements ContentIndex {

    private final Logger log = LoggerFactory.getLogger(EsContentIndex.class);

    private static final int DEFAULT_LIMIT = 50;

    private final Node esClient;
    private final String index;
    private final long requestTimeout;

    private Set<String> existingIndexes;

    private final EsQueryBuilder builder = new EsQueryBuilder();

    public EsContentIndex(Node esClient, String indexName, long requestTimeout) {
        this.esClient = checkNotNull(esClient);
        this.requestTimeout = requestTimeout;
        this.index = checkNotNull(indexName);
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
                timeoutGet(esClient.client().admin().indices().create(Requests.createIndexRequest(name)));
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

    private EsContent toEsContent(Item item) {
        return new EsContent()
            .id(item.getId().longValue())
            .type(ContentType.fromContent(item).get())
            .source(item.getSource() != null ? item.getSource().key() : null)
            .aliases(item.getAliases())
            .title(item.getTitle())
            .flattenedTitle(flattenedOrNull(item.getTitle()))
            .parentTitle(item.getTitle())
            .parentFlattenedTitle(flattenedOrNull(item.getTitle()))
            .specialization(item.getSpecialization() != null ? item.getSpecialization().name() : null)
            .broadcasts(makeESBroadcasts(item))
            .locations(makeESLocations(item))
            .topics(makeESTopics(item));
    }

    private void indexContainer(Container container) {
        EsContent indexed = new EsContent()
            .id(container.getId().longValue())
            .type(ContentType.fromContent(container).get())
            .source(container.getSource() != null ? container.getSource().key() : null)
            .aliases(container.getAliases())
            .title(container.getTitle())
            .flattenedTitle(flattenedOrNull(container.getTitle()))
            .parentTitle(container.getTitle())
            .parentFlattenedTitle(flattenedOrNull(container.getTitle()))
            .specialization(container.getSpecialization() != null ? container.getSpecialization().name() : null)
            .topics(makeESTopics(container));
        if (!container.getItemRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
            indexChildrenData(container);
        } else {
            indexed.hasChildren(Boolean.FALSE);
        }
        IndexRequest request = Requests.indexRequest(CONTENT_INDEX)
            .type(EsContent.TOP_LEVEL_CONTAINER)
            .id(getDocId(container))
            .source(indexed.toMap());
        timeoutGet(esClient.client().index(request));
        log.debug("indexed {}", container);
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

    private DateTime toUtc(DateTime transmissionTime) {
        return transmissionTime.toDateTime(DateTimeZones.UTC);
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

    private EsLocation toEsLocation(Policy policy) {
        return new EsLocation()
            .availabilityTime(toUtc(policy.getAvailabilityStart()).toDate())
            .availabilityEndTime(toUtc(policy.getAvailabilityEnd()).toDate());
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
    };

    private class RuntimeIndexException extends RuntimeException {

        public RuntimeIndexException(String message, Throwable cause) {
            super(message, cause);
        }

    }

    @Override
    public ListenableFuture<FluentIterable<Id>> query(AttributeQuerySet query,
        Iterable<Publisher> publishers, Selection selection) {
        SettableFuture<SearchResponse> response = SettableFuture.create();
        esClient.client()
            .prepareSearch(index)
            .setTypes(EsContent.CHILD_ITEM, EsContent.TOP_LEVEL_CONTAINER, EsContent.TOP_LEVEL_ITEM)
            .setQuery(builder.buildQuery(query))
            .addField(EsContent.ID)
            .addSort(SortBuilders.fieldSort(EsContent.TOPICS+"."+EsTopicMapping.SUPERVISED).order(SortOrder.DESC))
            .addSort(SortBuilders.fieldSort(EsContent.TOPICS+"."+EsTopicMapping.WEIGHTING).order(SortOrder.DESC))
            .setPostFilter(FiltersBuilder.buildForPublishers(EsContent.SOURCE, publishers))
            .setFrom(selection.getOffset())
            .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT))
            .execute(FutureSettingActionListener.setting(response));

        return Futures.transform(response, new Function<SearchResponse, FluentIterable<Id>>() {
            @Override
            public FluentIterable<Id> apply(SearchResponse input) {
                /*
                 * TODO: if 
                 *  selection.offset + selection.limit < totalHits
                 * then we have more: return for use with response. 
                 */
                return FluentIterable.from(input.getHits()).transform(new Function<SearchHit, Id>() {
                    @Override
                    public Id apply(SearchHit hit) {
                        Long id = hit.field(EsContent.ID).<Number>value().longValue();
                        return Id.valueOf(id);
                    }
                });
            }
        });
    }
}
