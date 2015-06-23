package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class IndexingContentReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    private static final int maxAttempts = 3;



    private final Logger log = LoggerFactory.getLogger(IndexingContentReadWriteWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;
    private final DirectAndExplicitEquivalenceMigrator equivalenceMigrator;
    @Nullable
    private final Timer messagesTimer;
    private final ContentIndex index;

    public IndexingContentReadWriteWorker(ContentResolver contentResolver, ContentWriter writer,
            DirectAndExplicitEquivalenceMigrator equivalenceMigrator, ContentIndex index, @Nullable MetricRegistry metricsRegistry) {
        this.index = checkNotNull(index);
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);
        this.equivalenceMigrator = checkNotNull(equivalenceMigrator);
        this.messagesTimer = (metricsRegistry != null ? checkNotNull(metricsRegistry.timer("IndexingContentReadWriteWorker")) : null);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        if (messagesTimer != null) {
            Timer.Context timer = messagesTimer.time();
            readAndWriteThenIndex(message.getUpdatedResource().getId());
            timer.stop();
        } else {
            readAndWriteThenIndex(message.getUpdatedResource().getId());
        }
    }

    private void readAndWriteThenIndex(Id id) {
        readAndWriteThenIndex(id, 0);
    }

    private void readAndWriteThenIndex(final Id id, final int attempt) {
        if (attempt >= maxAttempts) {
            throw new RuntimeException(String.format("Failed to write %s in %s attempts", id, maxAttempts));
        }
        ImmutableList<Id> ids = ImmutableList.of(id);
        log.trace("Attempt to read and write {}", id.toString());
        ListenableFuture<Resolved<Content>> resolved = contentResolver.resolveIds(ids);
        Futures.addCallback(resolved, new FutureCallback<Resolved<Content>>() {

            @Override
            public void onSuccess(Resolved<Content> result) {
                for (Content content : result.getResources()) {
                    try {
                        log.trace("writing content " + content);
                        writer.writeContent(content);
                        equivalenceMigrator.migrateEquivalence(content);
                        index.index(content);
                        log.trace("Finished writing content " + content);
                    } catch (MissingResourceException mre) {
                        log.warn("missing {} for {}, re-attempting", mre.getMissingId(), content);
                        readAndWriteThenIndex(mre.getMissingId());
                        readAndWriteThenIndex(id, attempt + 1);
                    } catch (WriteException we) {
                        log.error("failed to write " + id + "-" + content, we);
                    } catch (IndexException ie) {
                        log.error("Failed to index {} - {}", content.getId(), ie);
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Failed to resolve id" + id);
            }
        });
    }
}