package org.atlasapi.system.bootstrap.workers;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentWriter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.Worker;

public class ContentReadWriteWorker implements Worker<ResourceUpdatedMessage> {

    private static final int maxAttempts = 3;



    private final Logger log = LoggerFactory.getLogger(ContentReadWriteWorker.class);

    private final ContentResolver contentResolver;
    private final ContentWriter writer;
    private final ExplicitEquivalenceMigrator explicitEquivalenceMigrator;

    public ContentReadWriteWorker(ContentResolver contentResolver, ContentWriter writer,
            ExplicitEquivalenceMigrator explicitEquivalenceMigrator) {
        this.contentResolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(writer);
        this.explicitEquivalenceMigrator = checkNotNull(explicitEquivalenceMigrator);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        readAndWrite(message.getUpdatedResource().getId());
    }

    private void readAndWrite(Id id) {
        readAndWrite(id, 0);
    }

    private void readAndWrite(final Id id, final int attempt) {
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
                        explicitEquivalenceMigrator.migrateEquivalence(content);
                        log.trace("Finished writing content " + content);
                    } catch (MissingResourceException mre) {
                        log.warn("missing {} for {}, re-attempting", mre.getMissingId(), content);
                        readAndWrite(mre.getMissingId());
                        readAndWrite(id, attempt + 1);
                    } catch (WriteException we) {
                        log.error("failed to write " + id + "-" + content, we);
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