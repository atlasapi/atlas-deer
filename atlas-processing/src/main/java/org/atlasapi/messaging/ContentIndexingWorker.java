package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.util.Resolved;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class ContentIndexingWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ContentResolver contentResolver;
    private final ContentIndex contentIndex;

    public ContentIndexingWorker(ContentResolver contentResolver, ContentIndex contentIndex) {
        this.contentResolver = contentResolver;
        this.contentIndex = contentIndex;
    }

    @Override
    public void process(final ResourceUpdatedMessage message) {
        try {
            Resolved<Content> results = Futures.get(resolveContent(message), 1, TimeUnit.MINUTES, TimeoutException.class);
            Optional<Content> content = results.getResources().first();
            if (content.isPresent()) {
                Content source = content.get();
                log.debug("indexing {}", source);
                contentIndex.index(source);
            }
        } catch (TimeoutException | IndexException e) {
            log.error("iqqndexing error:", e);
        }
    }

    private ListenableFuture<Resolved<Content>> resolveContent(
            final ResourceUpdatedMessage message) {
        return contentResolver.resolveIds(ImmutableList.of(message.getUpdatedResource().getId()));
    }
}