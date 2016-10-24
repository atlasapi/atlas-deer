package org.atlasapi.messaging;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdatingWorker implements Worker<EquivalenceAssertionMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContentEquivalenceUpdatingWorker.class);

    private final EquivalenceGraphStore graphStore;
    private final DirectAndExplicitEquivalenceMigrator equivMigrator;

    private final MetricRegistry metricRegistry;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final String publisherMeterName;

    private ContentEquivalenceUpdatingWorker(
            EquivalenceGraphStore graphStore,
            DirectAndExplicitEquivalenceMigrator equivMigrator,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.graphStore = checkNotNull(graphStore);
        this.equivMigrator = checkNotNull(equivMigrator);

        this.metricRegistry = metricRegistry;

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");

        this.publisherMeterName = metricPrefix + "%s.meter.received";
    }

    public static ContentEquivalenceUpdatingWorker create(
            EquivalenceGraphStore graphStore,
            DirectAndExplicitEquivalenceMigrator equivMigrator,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new ContentEquivalenceUpdatingWorker(
                graphStore, equivMigrator, metricPrefix, metricRegistry
        );
    }

    @Override
    public void process(EquivalenceAssertionMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        metricRegistry.meter(
                String.format(
                        publisherMeterName,
                        message.getSubject().getSource()
                ))
                .mark();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processing message on id {}, took: PT{}S, asserted adjacents: {}, message: {}",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(MoreCollectors.toImmutableList()),
                    getTimeToProcessInSeconds(message),
                    message
            );
        }

        Timer.Context time = executionTimer.time();

        try {
            graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(),
                    message.getPublishers()
            );
            equivMigrator.migrateEquivalence(message.getSubject());


            LOG.debug("Successfully processed message {}", message.toString());
        } catch (Exception e) {
            LOG.warn(
                    "Failed to process message on id {}, asserted adjacents: {}, message: {}",
                    message.getSubject().getId(),
                    message.getAssertedAdjacents().stream()
                            .map(ResourceRef::getId)
                            .collect(MoreCollectors.toImmutableList()),
                    message
            );
            failureMeter.mark();

            throw Throwables.propagate(e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
