package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.util.Resolved;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class ContentIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ContentResolver contentResolver;
    private final ContentIndex contentIndex;
    private final Timer messageTimer;

    public ContentIndexingWorker(ContentResolver contentResolver, ContentIndex contentIndex,
                                 @Nullable MetricRegistry metrics) {
        this.contentResolver = contentResolver;
        this.contentIndex = contentIndex;
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("ContentIndexingWorker")) : null);
    }

    @Override
    public void process(final ResourceUpdatedMessage message) throws RecoverableException {
        log.debug("Processing message {}", message.toString());
        try {
            Timer.Context time = null;
            if (messageTimer != null) {
                time = messageTimer.time();
            }
            Resolved<Content> results = Futures.get(resolveContent(message), 30, TimeUnit.SECONDS, TimeoutException.class);
            Optional<Content> content = results.getResources().first();
            if (content.isPresent()) {
                Content source = content.get();
                log.debug("indexing {}", source);
                contentIndex.index(source);
            }
            if (time != null) {
                time.stop();
            }
        } catch (TimeoutException | IndexException e) {
            throw new RecoverableException("indexing error:", e);
        }
    }

    private ListenableFuture<Resolved<Content>> resolveContent(final ResourceUpdatedMessage message) {
        return contentResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }
}