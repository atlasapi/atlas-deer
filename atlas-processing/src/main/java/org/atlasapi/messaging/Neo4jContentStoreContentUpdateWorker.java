package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.neo4j.service.Neo4jContentStore;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

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
    private final Timer timer;

    private Neo4jContentStoreContentUpdateWorker(
            ContentResolver contentResolver,
            Neo4jContentStore neo4JContentStore,
            Timer timer
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.neo4JContentStore = checkNotNull(neo4JContentStore);
        this.timer = checkNotNull(timer);
    }

    public static Neo4jContentStoreContentUpdateWorker create(
            ContentResolver contentResolver,
            Neo4jContentStore neo4JContentStore,
            Timer timer
    ) {
        return new Neo4jContentStoreContentUpdateWorker(
                contentResolver, neo4JContentStore, timer
        );
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
        Id contentId = message.getContentRef().getId();

        log.debug("Processing message on id {}, took PT{}S, message: {}",
                contentId, getTimeToProcessInSeconds(message));

        Timer.Context time = timer.time();
        try {
            Content content = getContent(contentId);
            neo4JContentStore.writeContent(content);

            time.stop();
        } catch (Exception e) {
            throw Throwables.propagate(e);
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

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
