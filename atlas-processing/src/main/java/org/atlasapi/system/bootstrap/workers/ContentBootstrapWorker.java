package org.atlasapi.system.bootstrap.workers;

import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.Meter;
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
    private final Timer metricsTimer;
    private final Meter contentNotWrittenMeter;
    private final Meter failureMeter;

    private ContentBootstrapWorker(
            ContentResolver contentResolver,
            ContentWriter writer,
            Timer metricsTimer,
            Meter contentNotWrittenMeter,
            Meter failureMeter
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);
        this.metricsTimer = checkNotNull(metricsTimer);
        this.contentNotWrittenMeter = checkNotNull(contentNotWrittenMeter);
        this.failureMeter = checkNotNull(failureMeter);
    }

    public static ContentBootstrapWorker create(
            ContentResolver contentResolver,
            ContentWriter writer,
            Timer metricsTimer,
            Meter contentNotWrittenMeter,
            Meter failureMeter
    ) {
        return new ContentBootstrapWorker(
                contentResolver, writer, metricsTimer, contentNotWrittenMeter, failureMeter
        );
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        Id contentId = message.getUpdatedResource().getId();
        log.debug("Processing message on id {}, took: PT{}S, message: {}",
                contentId, getTimeToProcessInSeconds(message), message);
        try {
            process(contentId);
        } catch (Exception e) {
            log.error("Failed to bootstrap content {}", message.getUpdatedResource(), e);
            failureMeter.mark();
            throw Throwables.propagate(e);
        }
    }

    private void process(Id contentId) throws org.atlasapi.entity.util.WriteException {
        Timer.Context time = metricsTimer.time();
        Resolved<Content> content = resolveContent(contentId);

        if (content.getResources().isEmpty()) {
            log.error("Failed to resolve content {}", contentId);
            throw new IllegalArgumentException("Failed to resolve content " + contentId);
        }

        WriteResult<Content, Content> result =
                writer.writeContent(content.getResources().first().get());

        if (result.written()) {
            log.debug("Bootstrapped content {}", result.toString());
            time.stop();
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

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
