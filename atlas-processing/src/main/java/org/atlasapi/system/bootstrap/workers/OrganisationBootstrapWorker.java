package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class OrganisationBootstrapWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ContentBootstrapWorker.class);

    private final OrganisationResolver resolver;
    private final OrganisationWriter writer;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    @Nullable private final RateLimiter rateLimiter;

    private OrganisationBootstrapWorker(
            OrganisationResolver resolver,
            OrganisationWriter writer,
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

    public static OrganisationBootstrapWorker create(
            OrganisationResolver resolver,
            OrganisationWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry,
            @Nullable RateLimiter rateLimiter
    ) {
        return new OrganisationBootstrapWorker(resolver, writer, metricPrefix, metricRegistry, rateLimiter);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
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
            Id contentId = message.getUpdatedResource().getId();
            Resolved<Organisation> content = Futures.get(
                    resolver.resolveIds(ImmutableList.of(contentId)),
                    ExecutionException.class
            );
            Organisation organisation = content.getResources().first().get();

            writer.write(organisation);

            latencyTimer.update(
                    getTimeToProcessInMillis(message.getTimestamp()),
                    TimeUnit.MILLISECONDS
            );
            LOG.debug("Bootstrapped organisation {}", organisation.toString());
        } catch (Exception e) {
            LOG.error(
                    "Failed to bootstrap organisation {} - {} {}",
                    message.getUpdatedResource(),
                    e,
                    Throwables
                            .getStackTraceAsString(e)
            );
            failureMeter.mark();
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
