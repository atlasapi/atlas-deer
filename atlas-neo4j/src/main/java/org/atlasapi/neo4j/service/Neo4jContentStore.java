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
    private final Timer writeEquivalencesStartTransactionTimer;
    private final Timer writeEquivalencesContentRefsTimer;
    private final Timer writeEquivalencesEquivalencesTimer;
    private final Timer writeEquivalencesEndTransactionTimer;
    private final Meter writeEquivalencesFailureMeter;

    private final Timer writeContentOverallTimer;
    private final Timer writeContentStartTransactionTimer;
    private final Timer writeContentContentTimer;
    private final Timer writeContentHierarchyTimer;
    private final Timer writeContentLocationTimer;
    private final Timer writeContentBroadcastTimer;
    private final Timer writeContentEndTransactionTimer;
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
        this.writeEquivalencesStartTransactionTimer = metricRegistry.timer(
                WRITE_EQUIV_TIMER_PREFIX + "startTransaction"
        );
        this.writeEquivalencesContentRefsTimer = metricRegistry.timer(
                WRITE_EQUIV_TIMER_PREFIX + "contentRefs"
        );
        this.writeEquivalencesEquivalencesTimer = metricRegistry.timer(
                WRITE_EQUIV_TIMER_PREFIX + "equivalences"
        );
        this.writeEquivalencesEndTransactionTimer = metricRegistry.timer(
                WRITE_EQUIV_TIMER_PREFIX + "endTransaction"
        );
        this.writeEquivalencesFailureMeter = metricRegistry.meter(
                WRITE_EQUIV_METER_PREFIX + "failure"
        );

        this.writeContentOverallTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "overall"
        );
        this.writeContentStartTransactionTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "startTransaction"
        );
        this.writeContentContentTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "content"
        );
        this.writeContentHierarchyTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "hierarchy"
        );
        this.writeContentLocationTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "location"
        );
        this.writeContentBroadcastTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "broadcast"
        );
        this.writeContentEndTransactionTimer = metricRegistry.timer(
                WRITE_CONTENT_TIMER_PREFIX + "endTransaction"
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
                transaction -> writeEquivalences(subject, assertedAdjacents, sources, transaction),
                "write equivalences",
                writeEquivalencesStartTransactionTimer,
                writeEquivalencesEndTransactionTimer,
                writeEquivalencesFailureMeter
        );

        time.stop();
    }

    public void writeContent(Content content) {
        Timer.Context time = writeContentOverallTimer.time();

        executeInTransaction(
                transaction -> writeContent(content, transaction),
                "write content",
                writeContentStartTransactionTimer,
                writeContentEndTransactionTimer,
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
            Timer startTransactionTimer,
            Timer endTransactionTimer,
            Meter failureMeter
    ) {
        Timer.Context startTransactionTimerContext = startTransactionTimer.time();
        try (
                Session session = sessionFactory.getSession();
                Transaction transaction = session.beginTransaction()
        ) {
            startTransactionTimerContext.stop();

            try {
                consumer.accept(transaction);

                runWithTiming(
                        transaction::success,
                        startTransactionTimer
                );
            } catch (Exception e) {
                runWithTiming(
                        transaction::failure,
                        endTransactionTimer
                );
                failureMeter.mark();

                Throwables.propagateIfInstanceOf(e, Neo4jPersistenceException.class);
                throw Neo4jPersistenceException.create("Failed to " + consumerDescription, e);
            }
        }
    }

    private void writeEquivalences(ResourceRef subject, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources, Transaction transaction) {
        runWithTiming(
                () -> {
                    contentWriter.writeResourceRef(subject, transaction);
                    assertedAdjacents.forEach(
                            resourceRef -> contentWriter.writeResourceRef(resourceRef, transaction)
                    );
                },
                writeEquivalencesContentRefsTimer
        );

        runWithTiming(
                () -> graphWriter.writeEquivalences(
                        subject, assertedAdjacents, sources, transaction
                ),
                writeEquivalencesEquivalencesTimer
        );
    }

    private void writeContent(Content content, Transaction transaction) {
        content.accept(new ContentVisitor<Void>() {

            @Override
            public Void visit(Brand brand) {
                runWithTiming(
                        () -> contentWriter.writeContent(content, transaction),
                        writeContentContentTimer
                );
                runWithTiming(
                        () -> hierarchyWriter.writeBrand(brand, transaction),
                        writeContentHierarchyTimer
                );
                runWithTiming(
                        () -> locationWriter.write(brand, transaction),
                        writeContentLocationTimer
                );
                return null;
            }

            @Override
            public Void visit(Series series) {
                runWithTiming(
                        () -> contentWriter.writeSeries(series, transaction),
                        writeContentContentTimer
                );
                runWithTiming(
                        () -> hierarchyWriter.writeSeries(series, transaction),
                        writeContentHierarchyTimer
                );
                runWithTiming(
                        () -> locationWriter.write(series, transaction),
                        writeContentLocationTimer
                );
                return null;
            }

            @Override
            public Void visit(Episode episode) {
                runWithTiming(
                        () -> contentWriter.writeEpisode(episode, transaction),
                        writeContentContentTimer
                );
                runWithTiming(
                        () -> hierarchyWriter.writeEpisode(episode, transaction),
                        writeContentHierarchyTimer
                );
                runWithTiming(
                        () -> locationWriter.write(episode, transaction),
                        writeContentLocationTimer
                );
                runWithTiming(
                        () -> broadcastWriter.write(episode, transaction),
                        writeContentBroadcastTimer
                );
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
        runWithTiming(
                () -> contentWriter.writeContent(item, transaction),
                writeContentContentTimer
        );
        runWithTiming(
                () -> hierarchyWriter.writeNoHierarchy(item, transaction),
                writeContentHierarchyTimer
        );
        runWithTiming(
                () -> locationWriter.write(item, transaction),
                writeContentLocationTimer
        );
        runWithTiming(
                () -> broadcastWriter.write(item, transaction),
                writeContentBroadcastTimer
        );
    }

    private void runWithTiming(TimeableAction writer, Timer timer) {
        Timer.Context time = timer.time();
        writer.invoke();
        time.stop();
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

    private interface TimeableAction {

        void invoke();
    }
}
