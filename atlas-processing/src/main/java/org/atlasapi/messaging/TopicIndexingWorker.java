package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.IndexException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicIndex;
import org.atlasapi.topic.TopicResolver;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(TopicIndexingWorker.class);

    private final TopicResolver topicResolver;
    private final TopicIndex topicIndex;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;

    private TopicIndexingWorker(
            TopicResolver topicResolver,
            TopicIndex topicIndex,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.topicResolver = topicResolver;
        this.topicIndex = topicIndex;

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
    }

    public static TopicIndexingWorker create(
            TopicResolver topicResolver,
            TopicIndex topicIndex,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new TopicIndexingWorker(topicResolver, topicIndex, metricPrefix, metricRegistry);
    }

    @Override
    public void process(final ResourceUpdatedMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        LOG.debug(
                "Processing message on id {}, took: PT{}S, message: {}",
                message.getUpdatedResource(), getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message
        );

        Timer.Context time = executionTimer.time();

        try {
            @SuppressWarnings("Guava")
            Optional<Topic> topic = Futures.get(
                    resolveContent(message),
                    1,
                    TimeUnit.MINUTES,
                    TimeoutException.class
            )
                    .getResources()
                    .first();

            if (topic.isPresent()) {
                Topic source = topic.get();
                LOG.debug("indexing {}", source);
                topicIndex.index(source);

                latencyTimer.update(
                        getTimeToProcessInMillis(message.getTimestamp()),
                        TimeUnit.MILLISECONDS
                );
            } else {
                LOG.warn(
                        "{}: failed to resolve {} ",
                        new Object[] { message.getMessageId(), message.getUpdatedResource() }
                );
            }
        } catch (TimeoutException | IndexException e) {
            failureMeter.mark();
            throw new RecoverableException("indexing error:", e);
        } finally {
            time.stop();
        }
    }

    private ListenableFuture<Resolved<Topic>> resolveContent(final ResourceUpdatedMessage message) {
        return topicResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
