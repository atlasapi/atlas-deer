package org.atlasapi.system.health.probes;

import com.metabroadcast.common.health.probes.Probe;
import com.metabroadcast.common.health.probes.ProbeResult;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchProbe extends Probe {

    private static final long ES_TIMEOUT = 25L;
    private final Client esClient;

    private ElasticsearchProbe(String identifier, Client esClient) {
        super(identifier);
        this.esClient = checkNotNull(esClient);
    }

    public static ElasticsearchProbe create(
            String identifier,
            Client esClient
    ) {
        return new ElasticsearchProbe(identifier, esClient);
    }

    @Override
    public Callable<ProbeResult> createRequest() {
        return () -> {
            ActionFuture<ClusterHealthResponse> healthRequest = esClient.admin()
                    .cluster()
                    .health(new ClusterHealthRequest());

            try {
                ClusterHealthResponse response = healthRequest.actionGet(
                        ES_TIMEOUT,
                        TimeUnit.SECONDS
                );

                if (response.isTimedOut()) {
                    return ProbeResult.unhealthy(identifier, "request timed out");
                }
            } catch (ElasticsearchException e) {
                return ProbeResult.unhealthy(identifier, e);
            }

            return ProbeResult.healthy(identifier);
        };
    }

}
