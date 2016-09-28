package org.atlasapi.messaging;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentStoreGraphUpdateWorker
        implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentStoreGraphUpdateWorker.class);

    private final EquivalentContentStore equivalentContentStore;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;

    private EquivalentContentStoreGraphUpdateWorker(
            EquivalentContentStore equivalentContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
    }

    public static EquivalentContentStoreGraphUpdateWorker create(
            EquivalentContentStore equivalentContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentContentStoreGraphUpdateWorker(
                equivalentContentStore,
                metricPrefix,
                metricRegistry
        );
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processing message on updated graph: {}, took: PT{}S, "
                            + "created graph(s): {}, deleted graph(s): {}, message: {}",
                    message.getGraphUpdate().getUpdated().getId(),
                    message.getGraphUpdate().getCreated().stream()
                            .map(EquivalenceGraph::getId)
                            .collect(MoreCollectors.toImmutableList()),
                    message.getGraphUpdate().getDeleted(),
                    getTimeToProcessInSeconds(message),
                    message
            );
        }

        Timer.Context time = executionTimer.time();

        try {
            equivalentContentStore.updateEquivalences(message.getGraphUpdate());
        } catch (WriteException e) {
            LOG.warn(
                    "Failed to process message on updated graph: {}, created graph(s): {},"
                            + "deleted graph(s): {}, message: {}. Retrying...",
                    message.getGraphUpdate().getUpdated().getId(),
                    message.getGraphUpdate().getCreated().stream()
                            .map(EquivalenceGraph::getId)
                            .collect(MoreCollectors.toImmutableList()),
                    message.getGraphUpdate().getDeleted(),
                    message
            );
            failureMeter.mark();

            throw new RecoverableException("update failed for " + message.getGraphUpdate(), e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
