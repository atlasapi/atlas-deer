package org.atlasapi.messaging;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentStoreContentUpdateWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentStoreContentUpdateWorker.class);

    private final EquivalentContentStore equivalentContentStore;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final String publisherExecutionTimerName;
    private final String publisherMeterName;
    private final String publisherLatencyTimerName;

    private final MetricRegistry metricRegistry;
    private final RateLimiter rateLimiter;

    private EquivalentContentStoreContentUpdateWorker(
            EquivalentContentStore equivalentContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);

        this.metricRegistry = metricRegistry;

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
        this.publisherMeterName = metricPrefix + "source.%s.meter.received";
        this.publisherExecutionTimerName = metricPrefix + "source.%s.timer.execution";
        this.publisherLatencyTimerName = metricPrefix + "source.%s.timer.latency";
        String defaultRateLimit = System.getenv("EQUIVALENT_CONTENT_STORE_CONTENT_CONSUMER_MAX_MESSAGES_PER_SECOND");
        int rateLimit = Strings.isNullOrEmpty(defaultRateLimit)
                ? 1000 :
                Integer.parseInt(checkNotNull(defaultRateLimit));
        this.rateLimiter = RateLimiter.create(rateLimit);
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
        rateLimiter.acquire();
        long start = System.currentTimeMillis();

        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message
        );

        String metricSourceName = message.getUpdatedResource().getSource().key().replace('.', '_');

        metricRegistry.meter(String.format(publisherMeterName, metricSourceName)).mark();

        Timer publisherExecutionTimer = metricRegistry.timer(String.format(publisherExecutionTimerName, metricSourceName));
        Timer publisherLatencyTimer = metricRegistry.timer(String.format(publisherLatencyTimerName, metricSourceName));

        Timer.Context time = executionTimer.time();
        Timer.Context publisherTime = publisherExecutionTimer.time();

        try {
            equivalentContentStore.updateContent(message.getUpdatedResource().getId());

            long timeToProcessInMillis = getTimeToProcessInMillis(message.getTimestamp());

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);
            publisherLatencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long end = System.currentTimeMillis();
            LOG.info(
                    "Timings - Source: {}, Execution Time (ms): {}, Latency (ms): {}",
                    message.getUpdatedResource().getSource().key(),
                    end - start,
                    timeToProcessInMillis
            );
        } catch (WriteException e) {
            failureMeter.mark();

            throw new RecoverableException("update failed for content "
                    + message.getUpdatedResource(), e);
        } finally {
            time.stop();
            publisherTime.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
