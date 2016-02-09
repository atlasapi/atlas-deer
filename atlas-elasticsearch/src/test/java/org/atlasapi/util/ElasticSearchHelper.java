package org.atlasapi.util;

import java.util.Map;
import java.util.UUID;

import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchHelper {

    private ElasticSearchHelper() {
    }

    public static Node testNode() {
        return NodeBuilder.nodeBuilder()
                .local(true).clusterName(UUID.randomUUID().toString())
                .build().start();
    }

    public static void clearIndices(Client esClient) {
        IndicesStatusRequest req = Requests.indicesStatusRequest((String[]) null);
        IndicesStatusResponse statuses = indicesAdmin(esClient).status(req).actionGet();
        for (String index : statuses.getIndices().keySet()) {
            indicesAdmin(esClient).delete(Requests.deleteIndexRequest(index)).actionGet();
        }
    }

    private static IndicesAdminClient indicesAdmin(Client esClient) {
        return esClient.admin().indices();
    }

    public static void refresh(Client esClient) {
        Map<String, IndexStatus> indices = indicesAdmin(esClient)
                .status(Requests.indicesStatusRequest((String[]) null))
                .actionGet()
                .getIndices();
        indicesAdmin(esClient)
                .prepareRefresh(indices.keySet().toArray(new String[] {}))
                .execute()
                .actionGet();
    }

}
