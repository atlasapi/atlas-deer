package org.atlasapi.neo4j.service;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;
import org.atlasapi.neo4j.service.model.Neo4jPersistenceException;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;

import com.google.common.base.Throwables;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentGraphService {

    private static final Logger log = LoggerFactory.getLogger(ContentGraphService.class);

    private final Neo4jSessionFactory sessionFactory;
    private final EquivalenceWriter graphWriter;
    private final ContentWriter contentWriter;

    private ContentGraphService(
            Neo4jSessionFactory sessionFactory,
            EquivalenceWriter graphWriter,
            ContentWriter contentWriter
    ) {
        this.sessionFactory = checkNotNull(sessionFactory);
        this.graphWriter = checkNotNull(graphWriter);
        this.contentWriter = checkNotNull(contentWriter);
    }

    public static ContentGraphService create(
            Neo4jSessionFactory sessionFactory,
            EquivalenceWriter graphWriter,
            ContentWriter contentWriter
    ) {
        return new ContentGraphService(sessionFactory, graphWriter, contentWriter);
    }

    public void writeEquivalences(ResourceRef subject, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources) {

        try (Transaction transaction = sessionFactory.getSession().beginTransaction()) {
            try {
                contentWriter.writeResourceRef(subject, transaction);
                assertedAdjacents.forEach(
                        resourceRef -> contentWriter.writeResourceRef(resourceRef, transaction)
                );

                graphWriter.writeEquivalences(subject, assertedAdjacents, sources, transaction);
                transaction.success();
            } catch (Exception e) {
                log.error("Failed to write equivalences", e);
                transaction.failure();
                Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
                throw Neo4jPersistenceException.create("Failed to write equivalences", e);
            }
        }
    }
}
