package org.atlasapi.system.bootstrap.workers;

import java.util.concurrent.TimeUnit;

import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;
import org.atlasapi.topic.TopicWriter;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class TopicReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    public static final Logger LOG = LoggerFactory.getLogger(TopicReadWriteWorker.class);

    private final TopicResolver resolver;
    private final TopicWriter writer;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;

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
        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource().getId(), getTimeToProcessInSeconds(message), message
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
        } catch (Exception e) {
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
