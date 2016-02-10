package org.atlasapi.util;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.EsContent;

import com.google.common.base.Throwables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexCreator.class);
    private static final int REQUEST_TIMEOUT = 2500;

    public static boolean createContentIndex(Client esClient, String name) {
        checkNotNull(esClient);
        ActionFuture<IndicesExistsResponse> exists = esClient.admin().indices().exists(
                Requests.indicesExistsRequest(name)
        );
        if (!timeoutGet(exists).isExists()) {
            try {
                LOG.info("Creating index {}", name);
                timeoutGet(esClient.admin().indices().create(Requests.createIndexRequest(name)));
            } catch (IndexAlreadyExistsException e) {
                LOG.info("Index already exists: {}", e);
                return false;
            }
            return true;
        } else {
            LOG.info("Index {} exists", name);
            return false;
        }
    }

    public static void putTypeMapping(Client esClient, String... index) {
        try {
            LOG.info("Putting mapping for type {}", EsContent.TOP_LEVEL_CONTAINER);
            doMappingRequest(esClient, Requests.putMappingRequest(index)
                    .type(EsContent.TOP_LEVEL_CONTAINER)
                    .source(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_CONTAINER)));
            LOG.info("Putting mapping for type {}", EsContent.TOP_LEVEL_ITEM);
            doMappingRequest(esClient, Requests.putMappingRequest(index)
                    .type(EsContent.TOP_LEVEL_ITEM)
                    .source(EsContent.getTopLevelMapping(EsContent.TOP_LEVEL_ITEM)));
            LOG.info("Putting mapping for type {}", EsContent.CHILD_ITEM);
            doMappingRequest(esClient, Requests.putMappingRequest(index)
                    .type(EsContent.CHILD_ITEM)
                    .source(EsContent.getChildMapping()));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static void doMappingRequest(Client esClient, PutMappingRequest req) {
        try {
            timeoutGet(esClient.admin().indices().putMapping(req));
        } catch (MergeMappingException mme) {
            LOG.info("Merge Mapping Exception: {}/{}", req.indices(), req.type());
        }
    }

    private static <T> T timeoutGet(ActionFuture<T> future) {
        try {
            return future.actionGet(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (ElasticsearchException ese) {
            LOG.error("Elasticsearch error {}", ese);
            Throwable root = Throwables.getRootCause(ese);
            Throwables.propagateIfInstanceOf(root, ElasticsearchException.class);
            throw Throwables.propagate(ese);
        }
    }
}
