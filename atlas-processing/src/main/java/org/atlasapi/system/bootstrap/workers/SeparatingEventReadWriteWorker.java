package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class SeparatingEventReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(SeparatingEventReadWriteWorker.class);

    private final EventResolver resolver;
    private final EventWriter writer;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    @Nullable private final RateLimiter rateLimiter;

    private SeparatingEventReadWriteWorker(
            EventResolver resolver,
            EventWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            @Nullable RateLimiter rateLimiter
    ) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
        this.rateLimiter = rateLimiter;
        if (this.rateLimiter != null) {
            LOG.info("Limiting rate to a maximum of {} messages per second", this.rateLimiter.getRate());
        }
    }

    public static SeparatingEventReadWriteWorker create(
            EventResolver resolver,
            EventWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            @Nullable RateLimiter rateLimiter
    ) {
        return new SeparatingEventReadWriteWorker(resolver, writer, metricPrefix, metricRegistry, rateLimiter);
    }

    @Override
    public void process(ResourceUpdatedMessage message)
            throws RecoverableException {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                message
        );

        Timer.Context time = executionTimer.time();

        try {
            Event event = Futures.get(
                    resolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId())),
                    1,
                    TimeUnit.MINUTES,
                    Exception.class
            )
                    .getResources()
                    .first()
                    .get();

            writer.write(event);

            latencyTimer.update(
                    getTimeToProcessInMillis(message.getTimestamp()),
                    TimeUnit.MILLISECONDS
            );
        } catch (Exception e) {
            failureMeter.mark();
            throw Throwables.propagate(e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
