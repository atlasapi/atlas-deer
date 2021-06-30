package org.atlasapi.system.bootstrap.workers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopicReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    public static final Logger LOG = LoggerFactory.getLogger(TopicReadWriteWorker.class);

    private final TopicResolver resolver;
    private final TopicWriter writer;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final RateLimiter rateLimiter;

    private TopicReadWriteWorker(
            TopicResolver resolver,
            TopicWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);

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

    public static TopicReadWriteWorker create(
            TopicResolver resolver,
            TopicWriter writer,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new TopicReadWriteWorker(resolver, writer, metricPrefix, metricRegistry);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        rateLimiter.acquire();
        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                message
        );

        Timer.Context time = executionTimer.time();

        try {
            Topic topic = Futures.get(
                    resolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId())),
                    1,
                    TimeUnit.MINUTES,
                    Exception.class
            )
                    .getResources()
                    .first()
                    .get();

            writer.writeTopic(topic);

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
