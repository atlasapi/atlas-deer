package org.atlasapi.neo4j;

import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.neo4j.service.resolvers.EquivalentSetResolver;
import org.atlasapi.neo4j.service.writers.BroadcastWriter;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;
import org.atlasapi.neo4j.service.writers.HierarchyWriter;
import org.atlasapi.neo4j.service.writers.LocationWriter;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import org.neo4j.driver.v1.AuthTokens;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jModule {

    private final Neo4jSessionFactory sessionFactory;

    private Neo4jModule(Neo4jSessionFactory sessionFactory) {
        this.sessionFactory = checkNotNull(sessionFactory);
    }

    public static Neo4jModule create(String neo4jHost, int neo4jPort, int maxIdleSessions) {
        Neo4jSessionFactory sessionFactory = Neo4jSessionFactory.create(
                neo4jHost,
                neo4jPort,
                AuthTokens.none(),
                maxIdleSessions
        );
        return new Neo4jModule(sessionFactory);
    }

    public Neo4jContentStore neo4jContentStore(MetricRegistry metricRegistry) {
        return Neo4jContentStore.builder()
                .withSessionFactory(sessionFactory)
                .withGraphWriter(EquivalenceWriter.create())
                .withContentWriter(ContentWriter.create())
                .withBroadcastWriter(BroadcastWriter.create())
                .withLocationWriter(LocationWriter.create())
                .withHierarchyWriter(HierarchyWriter.create())
                .withEquivalentSetResolver(EquivalentSetResolver.create())
                .withMetricsRegistry(metricRegistry)
                .build();
    }

    public void close() {
        sessionFactory.close();
    }

    @VisibleForTesting
    public Neo4jSessionFactory sessionFactory() {
        return sessionFactory;
    }
}
