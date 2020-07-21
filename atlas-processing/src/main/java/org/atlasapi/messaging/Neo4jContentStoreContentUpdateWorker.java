package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.neo4j.service.Neo4jContentStore;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jContentStoreContentUpdateWorker
        implements Worker<EquivalentContentUpdatedMessage> {

    private static final Logger log =
            LoggerFactory.getLogger(Neo4jContentStoreContentUpdateWorker.class);

    private final ContentResolver contentResolver;
    private final Neo4jContentStore neo4JContentStore;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;

    private Neo4jContentStoreContentUpdateWorker(
            ContentResolver contentResolver,
            Neo4jContentStore neo4JContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.neo4JContentStore = checkNotNull(neo4JContentStore);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
    }

    public static Neo4jContentStoreContentUpdateWorker create(
            ContentResolver contentResolver,
            Neo4jContentStore neo4JContentStore,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new Neo4jContentStoreContentUpdateWorker(
                contentResolver, neo4JContentStore, metricPrefix, metricRegistry
        );
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        Id contentId = message.getContentRef().getId();

        log.debug("Processing message on id {}, took PT{}S, message: {}",
                contentId, getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message);

        Timer.Context time = executionTimer.time();
        try {
            Content content = getContent(contentId);
            neo4JContentStore.writeContent(content);

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

    private Content getContent(Id contentId) throws TimeoutException {
        Resolved<Content> results = Futures.get(
                resolveContent(contentId), 30, TimeUnit.SECONDS, TimeoutException.class
        );

        @SuppressWarnings("Guava")
        Optional<Content> contentOptional = results.getResources().first();

        if (!contentOptional.isPresent()) {
            throw WorkerException.create("Failed to resolve content " + contentId);
        }

        return contentOptional.get();
    }

    private ListenableFuture<Resolved<Content>> resolveContent(Id contentId) {
        return contentResolver.resolveIds(ImmutableList.of(contentId));
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
