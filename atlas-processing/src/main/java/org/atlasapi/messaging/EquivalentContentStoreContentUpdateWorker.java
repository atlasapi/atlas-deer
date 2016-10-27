package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentStoreContentUpdateWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentStoreContentUpdateWorker.class);

    private final EquivalentContentStore equivalentContentStore;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;

    private EquivalentContentStoreContentUpdateWorker(
            EquivalentContentStore equivalentContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
    }

    public static EquivalentContentStoreContentUpdateWorker create(
            EquivalentContentStore equivalentContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentContentStoreContentUpdateWorker(
                equivalentContentStore,
                metricPrefix,
                metricRegistry
        );
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message
        );

        Timer.Context time = executionTimer.time();

        try {
            equivalentContentStore.updateContent(message.getUpdatedResource().getId());

            latencyTimer.update(
                    getTimeToProcessInMillis(message.getTimestamp()),
                    TimeUnit.MILLISECONDS
            );
        } catch (WriteException e) {
            failureMeter.mark();

            throw new RecoverableException("update failed for content "
                    + message.getUpdatedResource(), e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
