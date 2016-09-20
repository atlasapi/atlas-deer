package org.atlasapi.neo4j.service;

import java.util.Set;
import java.util.function.Consumer;

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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This stores requires the following index/constraint to be manually generated on the Neo4j
 * cluster if it does not exist:
 * <p>
 * {@code CREATE CONSTRAINT ON (c:Content) ASSERT c.id IS UNIQUE}
 */
public class Neo4jContentStore {

    private static final String TIMER_PREFIX = "persistence.neo4j.contentStore.timer.";
    private static final String METER_PREFIX = "persistence.neo4j.contentStore.meter.";

    private static final String WRITE_EQUIV_TIMER_PREFIX = TIMER_PREFIX + "writeEquivalences.";
    private static final String WRITE_CONTENT_TIMER_PREFIX = TIMER_PREFIX + "writeContent.";

    private static final String WRITE_EQUIV_METER_PREFIX = METER_PREFIX + "writeEquivalences.";
    private static final String WRITE_CONTENT_METER_PREFIX = METER_PREFIX + "writeContent.";

    private final Neo4jSessionFactory sessionFactory;

    private final EquivalenceWriter graphWriter;
    private final ContentWriter contentWriter;
    private final BroadcastWriter broadcastWriter;
    private final LocationWriter locationWriter;
    private final HierarchyWriter hierarchyWriter;

    private final EquivalentSetResolver equivalentSetResolver;

    private final Timer writeEquivalencesOverallTimer;
    private final Meter writeEquivalencesFailureMeter;

    private final Timer writeContentOverallTimer;
    private final Meter writeContentFailureMeter;

    private Neo4jContentStore(
            Neo4jSessionFactory sessionFactory,
            EquivalenceWriter graphWriter,
            ContentWriter contentWriter,
            BroadcastWriter broadcastWriter,
            LocationWriter locationWriter,
            HierarchyWriter hierarchyWriter,
            EquivalentSetResolver equivalentSetResolver,
            MetricRegistry metricRegistry
    ) {
        this.sessionFactory = checkNotNull(sessionFactory);
        this.graphWriter = checkNotNull(graphWriter);
        this.contentWriter = checkNotNull(contentWriter);
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.locationWriter = checkNotNull(locationWriter);
        this.hierarchyWriter = checkNotNull(hierarchyWriter);
        this.equivalentSetResolver = checkNotNull(equivalentSetResolver);

        this.writeEquivalencesOverallTimer = metricRegistry.timer(
                WRITE_EQUIV_TIMER_PREFIX + "overall"
        );
        this.writeEquivalencesFailureMeter = metricRegistry.meter(
                WRITE_EQUIV_METER_PREFIX + "failure"
        );

        this.writeContentOverallTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "overall"
        );
        this.writeContentFailureMeter = metricRegistry.meter(
                WRITE_CONTENT_METER_PREFIX + "failure"
        );
    }

    public static SessionFactoryStep builder() {
        return new Builder();
    }

    public void writeEquivalences(ResourceRef subject, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources) {
        Timer.Context time = writeEquivalencesOverallTimer.time();

        executeInTransaction(
                transaction -> graphWriter.writeEquivalences(
                        subject, assertedAdjacents, sources, transaction
                ),
                "write equivalences",
                writeEquivalencesFailureMeter
        );

        time.stop();
    }

    public void writeContent(Content content) {
        Timer.Context time = writeContentOverallTimer.time();

        executeInTransaction(
                transaction -> writeContent(content, transaction),
                "write content",
                writeContentFailureMeter
        );

        time.stop();
    }

    public ImmutableSet<Id> getEquivalentSet(Id id) {
        try {
            return equivalentSetResolver.getEquivalentSet(id, sessionFactory.getSession());
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
            throw Neo4jPersistenceException.create("Failed to get graph for id " + id, e);
        }
    }

    private void executeInTransaction(
            Consumer<Transaction> consumer,
            String consumerDescription,
            Meter failureMeter
    ) {
        try (
                Session session = sessionFactory.getSession();
                Transaction transaction = session.beginTransaction()
        ) {
            try {
                consumer.accept(transaction);
                transaction.success();
            } catch (Exception e) {
                transaction.failure();
                failureMeter.mark();

                Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
                throw Neo4jPersistenceException.create("Failed to " + consumerDescription, e);
            }
        }
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

        MetricStep withEquivalentSetResolver(EquivalentSetResolver equivalentSetResolver);
    }

    public interface MetricStep {

        BuildStep withMetricsRegistry(MetricRegistry metricRegistry);
    }

    public interface BuildStep {

        Neo4jContentStore build();
    }

    public static class Builder implements SessionFactoryStep, GraphWriterStep, ContentWriterStep,
            BroadcastWriterStep, LocationWriterStep, HierarchyWriterStep, EquivalentSetResolverStep,
            MetricStep, BuildStep {

        private Neo4jSessionFactory sessionFactory;
        private EquivalenceWriter graphWriter;
        private ContentWriter contentWriter;
        private BroadcastWriter broadcastWriter;
        private LocationWriter locationWriter;
        private HierarchyWriter hierarchyWriter;
        private EquivalentSetResolver equivalentSetResolver;
        private MetricRegistry metricRegistry;

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
        public MetricStep withEquivalentSetResolver(EquivalentSetResolver equivalentSetResolver) {
            this.equivalentSetResolver = equivalentSetResolver;
            return this;
        }

        @Override
        public BuildStep withMetricsRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
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
                    this.equivalentSetResolver,
                    this.metricRegistry
            );
        }
    }
}
