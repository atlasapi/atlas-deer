package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.Id;
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

public class EquivalentContentIndexingWorker implements Worker<EquivalentContentUpdatedMessage> {

    private static final String METRICS_TIMER = "EquivalentContentIndexingWorker";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentResolver contentResolver;
    private final ContentIndex contentIndex;
    private final Timer timer;

    public EquivalentContentIndexingWorker(ContentResolver contentResolver,
            ContentIndex contentIndex, MetricRegistry metricRegistry) {
        this.contentResolver = checkNotNull(contentResolver);
        this.contentIndex = checkNotNull(contentIndex);
        this.timer = checkNotNull(metricRegistry.timer(METRICS_TIMER));
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
        log.debug("Processing message {}", message.toString());

        Id contentId = getContentId(message);

        Timer.Context time = timer.time();
        try {
            indexContent(contentId);
            time.stop();
        } catch (TimeoutException | IndexException e) {
            throw new RecoverableException("Failed to index content " + contentId, e);
        }
    }

    private Id getContentId(EquivalentContentUpdatedMessage message) {
        return message.getContentRef().getId();
    }

    private void indexContent(Id contentId) throws TimeoutException, IndexException {
        Resolved<Content> results = Futures.get(
                resolveContent(contentId), 30, TimeUnit.SECONDS, TimeoutException.class
        );
        Optional<Content> contentOptional = results.getResources().first();

        if (contentOptional.isPresent()) {
            Content content = contentOptional.get();
            log.debug("Indexing message {}", content);
            contentIndex.index(content);
        }
    }

    private ListenableFuture<Resolved<Content>> resolveContent(Id contentId) {
        return contentResolver.resolveIds(ImmutableList.of(contentId));
    }
}
