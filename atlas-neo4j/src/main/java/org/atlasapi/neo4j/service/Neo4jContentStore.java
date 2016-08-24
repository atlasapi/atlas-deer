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
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;
import org.atlasapi.neo4j.service.model.Neo4jPersistenceException;
import org.atlasapi.neo4j.service.resolvers.EquivalentSetResolver;
import org.atlasapi.neo4j.service.writers.BroadcastWriter;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;
import org.atlasapi.neo4j.service.writers.HierarchyWriter;
import org.atlasapi.neo4j.service.writers.LocationWriter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.v1.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jContentStore {

    private static final Logger log = LoggerFactory.getLogger(Neo4jContentStore.class);

    private final Neo4jSessionFactory sessionFactory;

    private final EquivalenceWriter graphWriter;
    private final ContentWriter contentWriter;
    private final BroadcastWriter broadcastWriter;
    private final LocationWriter locationWriter;
    private final HierarchyWriter hierarchyWriter;

    private final EquivalentSetResolver equivalentSetResolver;

    private Neo4jContentStore(
            Neo4jSessionFactory sessionFactory,
            EquivalenceWriter graphWriter,
            ContentWriter contentWriter,
            BroadcastWriter broadcastWriter,
            LocationWriter locationWriter,
            HierarchyWriter hierarchyWriter,
            EquivalentSetResolver equivalentSetResolver
    ) {
        this.sessionFactory = checkNotNull(sessionFactory);
        this.graphWriter = checkNotNull(graphWriter);
        this.contentWriter = checkNotNull(contentWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.locationWriter = checkNotNull(locationWriter);
        this.hierarchyWriter = checkNotNull(hierarchyWriter);
        this.equivalentSetResolver = checkNotNull(equivalentSetResolver);
    }

    public static SessionFactoryStep builder() {
        return new Builder();
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

    public ImmutableSet<Id> getEquivalentSet(Id id) {
        try {
            return equivalentSetResolver.getEquivalentSet(id, sessionFactory.getSession());
        } catch (Exception e) {
            log.error("Failed to get graph for id {}", id, e);
            Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
            throw Neo4jPersistenceException.create("Failed to get graph for id " + id, e);
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

    public interface SessionFactoryStep {

        GraphWriterStep withSessionFactory(Neo4jSessionFactory sessionFactory);
    }

    public interface GraphWriterStep {

        ContentWriterStep withGraphWriter(EquivalenceWriter graphWriter);
    }

    public interface ContentWriterStep {

        BroadcastWriterStep withContentWriter(ContentWriter contentWriter);
    }

    public interface BroadcastWriterStep {

        LocationWriterStep withBroadcastWriter(BroadcastWriter broadcastWriter);
    }

    public interface LocationWriterStep {

        HierarchyWriterStep withLocationWriter(LocationWriter locationWriter);
    }

    public interface HierarchyWriterStep {

        EquivalentSetResolverStep withHierarchyWriter(HierarchyWriter hierarchyWriter);
    }

    public interface EquivalentSetResolverStep {

        BuildStep withEquivalentSetResolver(EquivalentSetResolver equivalentSetResolver);
    }

    public interface BuildStep {

        Neo4jContentStore build();
    }

    public static class Builder
            implements SessionFactoryStep, GraphWriterStep, ContentWriterStep, BroadcastWriterStep,
            LocationWriterStep, HierarchyWriterStep, EquivalentSetResolverStep, BuildStep {

        private Neo4jSessionFactory sessionFactory;
        private EquivalenceWriter graphWriter;
        private ContentWriter contentWriter;
        private BroadcastWriter broadcastWriter;
        private LocationWriter locationWriter;
        private HierarchyWriter hierarchyWriter;
        private EquivalentSetResolver equivalentSetResolver;

        private Builder() {
        }

        @Override
        public GraphWriterStep withSessionFactory(Neo4jSessionFactory sessionFactory) {
            this.sessionFactory = sessionFactory;
            return this;
        }

        @Override
        public ContentWriterStep withGraphWriter(EquivalenceWriter graphWriter) {
            this.graphWriter = graphWriter;
            return this;
        }

        @Override
        public BroadcastWriterStep withContentWriter(ContentWriter contentWriter) {
            this.contentWriter = contentWriter;
            return this;
        }

        @Override
        public LocationWriterStep withBroadcastWriter(BroadcastWriter broadcastWriter) {
            this.broadcastWriter = broadcastWriter;
            return this;
        }

        @Override
        public HierarchyWriterStep withLocationWriter(LocationWriter locationWriter) {
            this.locationWriter = locationWriter;
            return this;
        }

        @Override
        public EquivalentSetResolverStep withHierarchyWriter(HierarchyWriter hierarchyWriter) {
            this.hierarchyWriter = hierarchyWriter;
            return this;
        }

        @Override
        public BuildStep withEquivalentSetResolver(EquivalentSetResolver equivalentSetResolver) {
            this.equivalentSetResolver = equivalentSetResolver;
            return this;
        }

        @Override
        public Neo4jContentStore build() {
            return new Neo4jContentStore(
                    this.sessionFactory,
                    this.graphWriter,
                    this.contentWriter,
                    this.broadcastWriter,
                    this.locationWriter,
                    this.hierarchyWriter,
                    this.equivalentSetResolver
            );
        }
    }
}
