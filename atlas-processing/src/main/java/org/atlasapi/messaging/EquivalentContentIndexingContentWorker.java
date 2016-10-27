package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

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

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentIndexingContentWorker
        implements Worker<EquivalentContentUpdatedMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentIndexingContentWorker.class);

    private final ContentResolver contentResolver;
    private final ContentIndex contentIndex;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;

    private EquivalentContentIndexingContentWorker(
            ContentResolver contentResolver,
            ContentIndex contentIndex,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.contentIndex = checkNotNull(contentIndex);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
    }

    public static EquivalentContentIndexingContentWorker create(
            ContentResolver contentResolver,
            ContentIndex contentIndex,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentContentIndexingContentWorker(
                contentResolver, contentIndex, metricPrefix, metricRegistry
        );
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        Id contentId = getContentId(message);

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                contentId, getTimeToProcessInMillis(message.getTimestamp()) / 1000L, message);

        Timer.Context time = executionTimer.time();

        try {
            indexContent(contentId, message.getTimestamp());
        } catch (TimeoutException | IndexException e) {
            failureMeter.mark();
            throw new RecoverableException("Failed to index content " + contentId, e);
        } finally {
            time.stop();
        }
    }

    private Id getContentId(EquivalentContentUpdatedMessage message) {
        return message.getContentRef().getId();
    }

    private void indexContent(Id contentId, Timestamp timestamp)
            throws TimeoutException, IndexException {
        Resolved<Content> results = Futures.get(
                resolveContent(contentId), 30, TimeUnit.SECONDS, TimeoutException.class
        );
        @SuppressWarnings("Guava")
        Optional<Content> contentOptional = results.getResources().first();

        if (contentOptional.isPresent()) {
            Content content = contentOptional.get();
            LOG.debug("Indexing message {}", content);
            contentIndex.index(content);

            latencyTimer.update(
                    getTimeToProcessInMillis(timestamp),
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private ListenableFuture<Resolved<Content>> resolveContent(Id contentId) {
        return contentResolver.resolveIds(ImmutableList.of(contentId));
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
