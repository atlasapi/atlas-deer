package org.atlasapi.system.bootstrap.workers;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.system.bootstrap.ColumbusTelescopeReporter;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final MetricRegistry metricRegistry;

    private ContentBootstrapWorker(
            ContentResolver contentResolver,
            ContentWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            ColumbusTelescopeReporter columbusTelescopeReporter
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.contentNotWrittenMeter = metricRegistry.meter(metricPrefix + "meter.nop");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
        this.publisherMeterName = metricPrefix + "source.%s.meter.received";

        this.metricRegistry = metricRegistry;

        this.columbusTelescopeReporter = checkNotNull(columbusTelescopeReporter);
    }

    public static ContentBootstrapWorker create(
            ContentResolver contentResolver,
            ContentWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            ColumbusTelescopeReporter columbusTelescopeReporter
    ) {
        return new ContentBootstrapWorker(
                contentResolver,
                writer,
                metricPrefix,
                metricRegistry,
                columbusTelescopeReporter
        );
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        Id contentId = message.getUpdatedResource().getId();
        log.debug("Processing message on id {}, took: PT{}S, message: {}",
                contentId, getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message);

        Timer.Context time = executionTimer.time();

        metricRegistry.meter(
                String.format(
                        publisherMeterName,
                        message.getUpdatedResource().getSource().key().replace('.', '_')
                )
        ).mark();

        try {
            process(contentId, message.getTimestamp());
        } catch (Exception e) {
            log.error("Failed to bootstrap content {}", message.getUpdatedResource(), e);
            failureMeter.mark();

            throw Throwables.propagate(e);
        } finally {
            time.stop();
        }
    }

    private void process(Id contentId, Timestamp timestamp) throws WriteException {
        Resolved<Content> content = resolveContent(contentId);

        if (content.getResources().isEmpty()) {
            log.error("Failed to resolve content {}", contentId);
            throw new IllegalArgumentException("Failed to resolve content " + contentId);
        }

        WriteResult<Content, Content> result =
                writer.writeContent(content.getResources().first().get());

        if (result.written()) {
            log.debug("Bootstrapped content {}", result.toString());

            latencyTimer.update(
                    getTimeToProcessInMillis(timestamp),
                    TimeUnit.MILLISECONDS
            );
            columbusTelescopeReporter.reportSuccessfulMigration(
                    content.getResources().first().get()
            );
        } else {
            log.debug("Content has not been written: {}", result.toString());
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
