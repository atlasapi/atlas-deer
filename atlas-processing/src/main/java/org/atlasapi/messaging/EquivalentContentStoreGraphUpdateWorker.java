package org.atlasapi.messaging;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentStoreGraphUpdateWorker
        implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentStoreGraphUpdateWorker.class);

    private final EquivalentContentStore equivalentContentStore;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final RateLimiter rateLimiter;

    private EquivalentContentStoreGraphUpdateWorker(
            EquivalentContentStore equivalentContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
                String defaultRateLimit = System.getenv("DEFAULT_CONSUMER_MAX_MESSAGES_PER_SECOND");
        int rateLimit = Strings.isNullOrEmpty(defaultRateLimit)
                ? 1000 :
                Integer.parseInt(checkNotNull(defaultRateLimit));
        this.rateLimiter = RateLimiter.create(rateLimit);
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
        rateLimiter.acquire();
        long start = System.currentTimeMillis();
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
                    getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                    message
            );
        }

        Timer.Context time = executionTimer.time();

        try {
            equivalentContentStore.updateEquivalences(message.getGraphUpdate());

            long timeToProcessInMillis = getTimeToProcessInMillis(message.getTimestamp());

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long end = System.currentTimeMillis();
            LOG.info(
                    "Timings - Execution Time (ms): {}, Latency (ms): {}",
                    end - start,
                    timeToProcessInMillis
            );
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

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
