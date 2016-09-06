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

    private static final String TIMER_PREFIX = "persistence.neo4j.";

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
        ContentWriter contentWriter = ContentWriter.create(
                metricRegistry.timer(TIMER_PREFIX + "contentWriter.writeResourceRef"),
                metricRegistry.timer(TIMER_PREFIX + "contentWriter.writeContentRef"),
                metricRegistry.timer(TIMER_PREFIX + "contentWriter.writeContent")
        );

        return Neo4jContentStore.builder()
                .withSessionFactory(sessionFactory)
                .withGraphWriter(EquivalenceWriter.create(
                        metricRegistry.timer(TIMER_PREFIX + "equivalenceWriter.writeEquivalence")
                ))
                .withContentWriter(contentWriter)
                .withBroadcastWriter(BroadcastWriter.create(
                        metricRegistry.timer(TIMER_PREFIX + "broadcastWriter.writeBroadcast")
                ))
                .withLocationWriter(LocationWriter.create(
                        metricRegistry.timer(TIMER_PREFIX + "locationWriter.writeLocation")
                ))
                .withHierarchyWriter(HierarchyWriter.create(
                        contentWriter,
                        metricRegistry.timer(TIMER_PREFIX + "hierarchyWriter.writeHierarchy")
                ))
                .withEquivalentSetResolver(EquivalentSetResolver.create())
                .withTimers(
                        metricRegistry.timer(TIMER_PREFIX + "contentStore.writeEquivalences"),
                        metricRegistry.timer(TIMER_PREFIX + "contentStore.writeContent")
                )
                .build();
    }

    public void close() {
        sessionFactory.close();
    }

    @VisibleForTesting
    Neo4jSessionFactory sessionFactory() {
        return sessionFactory;
    }
}
