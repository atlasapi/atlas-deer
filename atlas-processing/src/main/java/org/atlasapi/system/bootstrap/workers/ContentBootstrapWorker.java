package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.system.bootstrap.ColumbusTelescopeReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentBootstrapWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger log = LoggerFactory.getLogger(ContentBootstrapWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;
    private final ColumbusTelescopeReporter columbusTelescopeReporter;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter contentNotWrittenMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final String publisherMeterName;
    private final String publisherExecutionTimerName;
    private final String publisherLatencyTimerName;

    private final MetricRegistry metricRegistry;
    @Nullable private final RateLimiter rateLimiter;

    private ContentBootstrapWorker(
            ContentResolver contentResolver,
            ContentWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            ColumbusTelescopeReporter columbusTelescopeReporter,
            @Nullable RateLimiter rateLimiter
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.contentNotWrittenMeter = metricRegistry.meter(metricPrefix + "meter.nop");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
        this.publisherMeterName = metricPrefix + "source.%s.meter.received";
        this.publisherExecutionTimerName = metricPrefix + "source.%s.timer.execution";
        this.publisherLatencyTimerName = metricPrefix + "source.%s.timer.latency";

        this.metricRegistry = metricRegistry;

        this.columbusTelescopeReporter = checkNotNull(columbusTelescopeReporter);
        this.rateLimiter = rateLimiter;
        if (this.rateLimiter != null) {
            log.info("Limiting rate to a maximum of {} messages per second", this.rateLimiter.getRate());
        }
    }

    public static ContentBootstrapWorker create(
            ContentResolver contentResolver,
            ContentWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            ColumbusTelescopeReporter columbusTelescopeReporter,
            @Nullable RateLimiter rateLimiter
    ) {
        return new ContentBootstrapWorker(
                contentResolver,
                writer,
                metricPrefix,
                metricRegistry,
                columbusTelescopeReporter,
                rateLimiter
        );
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        long start = System.currentTimeMillis();
        messageReceivedMeter.mark();

        Id contentId = message.getUpdatedResource().getId();
        log.debug("Processing message on id {}, took: PT{}S, message: {}",
                contentId, getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message);

        String metricSourceName = message.getUpdatedResource().getSource().key().replace('.', '_');

        metricRegistry.meter(String.format(publisherMeterName, metricSourceName)).mark();

        Timer publisherExecutionTimer = metricRegistry.timer(String.format(publisherExecutionTimerName, metricSourceName));

        Timer.Context time = executionTimer.time();
        Timer.Context publisherTime = publisherExecutionTimer.time();

        try {
            process(contentId, message.getTimestamp(), message.getUpdatedResource().getSource(), start);
        } catch (Exception e) {
            log.error("Failed to bootstrap content {}", message.getUpdatedResource(), e);
            failureMeter.mark();

            throw Throwables.propagate(e);
        } finally {
            time.stop();
            publisherTime.stop();
        }
    }

    private void process(Id contentId, Timestamp timestamp, Publisher publisher, long processingStartTime) throws WriteException {
        String metricSourceName = publisher.key().replace('.', '_');

        Resolved<Content> content = resolveContent(contentId);

        if (content.getResources().isEmpty()) {
            log.error("Failed to resolve content {}", contentId);
            throw new IllegalArgumentException("Failed to resolve content " + contentId);
        }

        WriteResult<Content, Content> result = null;

        Content contentToWrite = content.getResources().first().get();

        try {
            result = writer.writeContent(contentToWrite);
        } catch (WriteException e) {
            Throwable cause = e.getCause();
            if (cause instanceof MissingResourceException) {
                MissingResourceException mre = (MissingResourceException) e.getCause();

                Id containerId = mre.getMissingId();
                log.info("Missing container {} for {}, trying to fix", containerId, contentId);

                Resolved<Content> missing = resolveContent(containerId);
                if (missing.getResources().isEmpty()) {
                    throw e;
                }

                writer.writeContent(missing.getResources().first().get());
                result = writer.writeContent(contentToWrite);
            } else {
                throw e;
            }
        }

        if (result.written()) {
            log.debug("Bootstrapped content {}", result.toString());

            long timeToProcessInMillis = getTimeToProcessInMillis(timestamp);

            Timer publisherLatencyTimer = metricRegistry.timer(String.format(publisherLatencyTimerName, metricSourceName));

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);
            publisherLatencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long processingEndTime = System.currentTimeMillis();
            log.info(
                    "Timings: Source: {}, Execution Time (ms): {}, Latency (ms): {}",
                    publisher.key(),
                    processingEndTime - processingStartTime,
                    timeToProcessInMillis
            );

            columbusTelescopeReporter.reportSuccessfulMigration(
                    contentToWrite
            );
        } else {
            log.debug("Content has not been written: {}", result);
            contentNotWrittenMeter.mark();
        }
    }

    private Resolved<Content> resolveContent(Id contentId) {
        try {
            return Futures.get(
                    contentResolver.resolveIds(ImmutableList.of(contentId)),
                    ExecutionException.class
            );
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
