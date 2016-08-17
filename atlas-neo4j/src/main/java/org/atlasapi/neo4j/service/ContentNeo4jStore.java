package org.atlasapi.neo4j.service;

import java.util.Set;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;
import org.atlasapi.neo4j.service.model.Neo4jPersistenceException;
import org.atlasapi.neo4j.service.writers.BroadcastWriter;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;
import org.atlasapi.neo4j.service.writers.HierarchyWriter;
import org.atlasapi.neo4j.service.writers.LocationWriter;

import com.google.common.base.Throwables;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentNeo4jStore {

    private static final Logger log = LoggerFactory.getLogger(ContentNeo4jStore.class);

    private final Neo4jSessionFactory sessionFactory;
    private final EquivalenceWriter graphWriter;
    private final ContentWriter contentWriter;
    private final BroadcastWriter broadcastWriter;
    private final LocationWriter locationWriter;
    private final HierarchyWriter hierarchyWriter;

    private ContentNeo4jStore(
            Neo4jSessionFactory sessionFactory,
            EquivalenceWriter graphWriter,
            ContentWriter contentWriter,
            BroadcastWriter broadcastWriter,
            LocationWriter locationWriter,
            HierarchyWriter hierarchyWriter
    ) {
        this.sessionFactory = checkNotNull(sessionFactory);
        this.graphWriter = checkNotNull(graphWriter);
        this.contentWriter = checkNotNull(contentWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.locationWriter = checkNotNull(locationWriter);
        this.hierarchyWriter = checkNotNull(hierarchyWriter);
    }

    public static ContentNeo4jStore create(
            Neo4jSessionFactory sessionFactory,
            EquivalenceWriter graphWriter,
            ContentWriter contentWriter,
            BroadcastWriter broadcastWriter,
            LocationWriter locationWriter,
            HierarchyWriter hierarchyWriter
    ) {
        return new ContentNeo4jStore(
                sessionFactory,
                graphWriter,
                contentWriter,
                broadcastWriter,
                locationWriter,
                hierarchyWriter
        );
    }

    public void writeEquivalences(ResourceRef subject, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources) {
        try (Transaction transaction = sessionFactory.getSession().beginTransaction()) {
            try {
                writeEquivalences(subject, assertedAdjacents, sources, transaction);
                transaction.success();
            } catch (Exception e) {
                log.error("Failed to write equivalences", e);
                transaction.failure();
                Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
                throw Neo4jPersistenceException.create("Failed to write equivalences", e);
            }
        }
    }

    public void writeContent(Content content) {
        try (Transaction transaction = sessionFactory.getSession().beginTransaction()) {
            try {
                writeContent(content, transaction);
                transaction.success();
            } catch (Exception e) {
                log.error("Failed to write content", e);
                transaction.failure();
                Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
                throw Neo4jPersistenceException.create("Failed to write content", e);
            }
        }
    }

    private void writeEquivalences(ResourceRef subject, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources, Transaction transaction) {
        contentWriter.writeResourceRef(subject, transaction);
        assertedAdjacents.forEach(
                resourceRef -> contentWriter.writeResourceRef(resourceRef, transaction)
        );

        graphWriter.writeEquivalences(subject, assertedAdjacents, sources, transaction);
    }

    private void writeContent(Content content, Transaction transaction) {
        content.accept(new ContentVisitor<Void>() {

            @Override
            public Void visit(Brand brand) {
                contentWriter.writeContent(content, transaction);
                hierarchyWriter.writeBrand(brand, transaction);
                locationWriter.write(brand, transaction);
                return null;
            }

            @Override
            public Void visit(Series series) {
                contentWriter.writeSeries(series, transaction);
                hierarchyWriter.writeSeries(series, transaction);
                locationWriter.write(series, transaction);
                return null;
            }

            @Override
            public Void visit(Episode episode) {
                contentWriter.writeEpisode(episode, transaction);
                hierarchyWriter.writeEpisode(episode, transaction);
                locationWriter.write(episode, transaction);
                broadcastWriter.write(episode, transaction);
                return null;
            }

            @Override
            public Void visit(Film film) {
                writeItem(film, transaction);
                return null;
            }

            @Override
            public Void visit(Song song) {
                writeItem(song, transaction);
                return null;
            }

            @Override
            public Void visit(Item item) {
                writeItem(item, transaction);
                return null;
            }

            @Override
            public Void visit(Clip clip) {
                writeItem(clip, transaction);
                return null;
            }
        });
    }

    private void writeItem(Item item, Transaction transaction) {
        contentWriter.writeContent(item, transaction);
        hierarchyWriter.writeNoHierarchy(item, transaction);
        locationWriter.write(item, transaction);
        broadcastWriter.write(item, transaction);
    }
}
