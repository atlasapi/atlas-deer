package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.EsSchema;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.util.ElasticsearchUtils;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.exists.ExistsRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /* We unindex content before inserting any new records to avoid duplicates when a piece of
        content changes type (e.g. from top level to child)
     */
    public void index(Content content) throws IndexException {
        removeContent(content);
        if (!content.isActivelyPublished()) {
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

    private void removeContent(Content content) {
        Long id = content.getId().longValue();
        log.debug("Content {} is not actively published, removing from index", id);
        if (content instanceof Item) {
            deleteFromIndexIfExists(id, EsContent.CHILD_ITEM);
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_ITEM);
        }
        if (content instanceof Container) {
            deleteFromIndexIfExists(id, EsContent.TOP_LEVEL_CONTAINER);
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
            response.getSourceAsMap().put(EsContent.CANONICAL_ID, canonicalId.longValue());
            bulkReq.add(Requests.indexRequest(indexName)
                            .id(Integer.toString((int) response.getSourceAsMap().get(EsContent.ID)))
                            .type(response.getType())
                            .source(response.getSourceAsMap())
            );
        }
        BulkResponse resp = ElasticsearchUtils.getWithTimeout(esClient.bulk(bulkReq), requestTimeout);
        if (resp.hasFailures()) {
            throw new IndexException("Failures occurred while bulk updating canonical IDs: " + resp.buildFailureMessage());
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