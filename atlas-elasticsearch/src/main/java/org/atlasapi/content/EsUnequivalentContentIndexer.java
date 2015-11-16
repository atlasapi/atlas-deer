package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.util.ElasticsearchUtils;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class EsUnequivalentContentIndexer {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Client esClient;
    private final ContentResolver contentResolver;
    private final String indexName;
    private final Integer requestTimeout;
    private final ChannelGroupResolver cgResolver;
    private final SecondaryIndex equivIdIndex;
    private final EsContentTranslator contentTranslator;

    public EsUnequivalentContentIndexer(
            Client esClient,
            ContentResolver contentResolver,
            String indexName,
            Integer requestTimeout,
            ChannelGroupResolver cgResolver,
            SecondaryIndex equivIdIndex,
            EsContentTranslator contentTranslator) {
        this.contentTranslator = checkNotNull(contentTranslator);
        this.esClient = checkNotNull(esClient);
        this.contentResolver = checkNotNull(contentResolver);
        this.indexName = checkNotNull(indexName);
        this.requestTimeout = checkNotNull(requestTimeout);
        this.cgResolver = checkNotNull(cgResolver);
        this.equivIdIndex = checkNotNull(equivIdIndex);

    }

    public void index(Content content) throws IndexException {
        removeStaleContent(content);
        if (!content.isActivelyPublished()) {
            removeContent(content);
            return;
        }
        try {
            content.accept(getIndexingVisitor());
        } catch (RuntimeIndexException e) {
            throw new IndexException(e);
        }
    }

    private ContentVisitorAdapter<Void> getIndexingVisitor() {
        return new ContentVisitorAdapter<Void>() {

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
        };
    }

    private void removeStaleContent(Content content) {
        // Remove from index entries with the same ID, but different mapping type to avoid
        // duplicates when a piece of content changes type (e.g. from top-level to child)

        Long id = content.getId().longValue();
        if (content instanceof Container) {
            deleteFromIndexIfExists(id, EsContent.CHILD_ITEM);
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_ITEM);
        }
        else if (content instanceof Episode) {
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_CONTAINER);
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_ITEM);
        }
        else if (content instanceof Item) {
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_CONTAINER);
            deleteFromIndexIfExists(id, EsContent.CHILD_ITEM);
        }
    }

    private void removeContent(Content content) {
        long id = content.getId().longValue();
        if (content instanceof Container) {
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_CONTAINER);
        }
        else if (content instanceof Episode) {
            deleteFromIndexIfExists(id, EsContent.CHILD_ITEM);
        }
        else if (content instanceof Item) {
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_ITEM);
        }
    }

    private void deleteFromIndexIfExists(Long id, String mappingType) {
        try {
            boolean exists = new ExistsRequestBuilder(esClient)
                    .setTypes(mappingType)
                    .setQuery(QueryBuilders.termQuery(EsContent.ID, id.toString()))
                    .execute()
                    .get()
                    .exists();
            if (exists) {
                log.debug("Deleting content {} from index", id);
                esClient.delete(new DeleteRequest(indexName, mappingType, id.toString())).get();
            }
        } catch (Exception e) {
            log.error("Failed to delete content {} due to {}", id, e);
        }
    }

    public void indexContainer(Container container) {
        try {
            EsContent indexed = contentTranslator.toEsContainer(container);
            IndexRequest request = Requests.indexRequest(indexName)
                    .type(EsContent.TOP_LEVEL_CONTAINER)
                    .id(getDocId(container))
                    .source(indexed.toMap());
            ElasticsearchUtils.getWithTimeout(esClient.index(request), requestTimeout);
            log.debug("indexed {}", container);
        } catch (Exception e) {
            throw new RuntimeIndexException("Error indexing " + container, e);
        }
    }

    public void indexItem(Item item) {
        try {
            EsContent esContent = contentTranslator.toEsContent(item);
            BulkRequest requests = Requests.bulkRequest();
            IndexRequest mainIndexRequest;
            ContainerRef container = item.getContainerRef();
            if (item instanceof Episode) {
                contentTranslator.denormalizeEpisodeOntoSeries(item);
            }
            if (container != null) {
                contentTranslator.setParentFields(esContent, container);
                mainIndexRequest = Requests.indexRequest(indexName)
                        .type(EsContent.CHILD_ITEM)
                        .id(getDocId(item))
                        .source(esContent.toMap())
                        .parent(getDocId(container));
                log.debug(mainIndexRequest.source().toUtf8());
            } else {
                mainIndexRequest = Requests.indexRequest(indexName)
                        .type(EsContent.TOP_LEVEL_ITEM)
                        .id(getDocId(item))
                        .source(esContent.hasChildren(false).toMap());
                log.debug(mainIndexRequest.source().toUtf8());
            }

            requests.add(mainIndexRequest);
            BulkResponse resp = ElasticsearchUtils.getWithTimeout(esClient.bulk(requests), requestTimeout);
            log.debug("indexed {} ({}ms)", item, resp.getTookInMillis());
        } catch (Exception e) {
            throw new RuntimeIndexException("Error indexing " + item, e);
        }
    }



    private String getDocId(Content content) {
        return String.valueOf(content.getId());
    }

    private String getDocId(ContainerRef container) {
        return String.valueOf(container.getId());
    }

    public void updateCanonicalIds(Id canonicalId, Iterable<Id> setIds) throws IndexException {
        ImmutableSet<GetResponse> setSources = resolveSet(setIds);
        BulkRequest bulkReq = Requests.bulkRequest();
        for (GetResponse response : setSources) {
            Map<String, Object> source = response.getSourceAsMap();
            if (source != null) {
                source.put(EsContent.CANONICAL_ID, canonicalId.longValue());
                IndexRequest req = Requests.indexRequest(indexName)
                        .id(Integer.toString((int) response.getSourceAsMap().get(EsContent.ID)))
                        .type(response.getType())
                        .source(response.getSourceAsMap());
                if (response.getType().equalsIgnoreCase(EsContent.CHILD_ITEM)) {
                    req.parent(String.valueOf(source.get(EsContent.BRAND)));
                }
                bulkReq.add(req);
            }
        }
        if (bulkReq.numberOfActions() > 0) {
            BulkResponse resp = ElasticsearchUtils.getWithTimeout(esClient.bulk(bulkReq), requestTimeout);
            if (resp.hasFailures()) {
                throw new IndexException("Failures occurred while bulk updating canonical IDs: " + resp.buildFailureMessage());
            }
        }
    }

    private ImmutableSet<GetResponse> resolveSet(Iterable<Id> setIds) {
        ImmutableSet.Builder<GetResponse> builder = ImmutableSet.builder();
        for (Id setId : setIds) {
            ActionFuture<GetResponse> req = esClient.get(
                    Requests.getRequest(EsSchema.CONTENT_INDEX)
                            .id(setId.toString())
            );
            builder.add(req.actionGet());
        }
        return builder.build();
    }
}

