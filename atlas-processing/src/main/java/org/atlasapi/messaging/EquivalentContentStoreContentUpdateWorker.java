package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.RecoverableException;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.Worker;

public class EquivalentContentStoreContentUpdateWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ContentResolver contentResolver;

    private final EquivalentContentStore equivalentContentStore;
    private final Timer messageTimer;

    public EquivalentContentStoreContentUpdateWorker(
            EquivalentContentStore equivalentContentStore,
            ContentResolver contentResolver,
            MetricRegistry metrics
    ) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.contentResolver = checkNotNull(contentResolver);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("EquivalentContentStoreContentUpdateWorker")) : null);
    }

    @Override
    public void process(ResourceUpdatedMessage message) throws RecoverableException {
        try {
            Timer.Context timer = messageTimer.time();
            ListenableFuture<Resolved<Content>> contentFuture = contentResolver.resolveIds(
                    ImmutableList.of(message.getUpdatedResource().getId())
            );
            //this should never be empty, as the message here has been sent for existing content
            Content content = Futures.get(
                    contentFuture,
                    RecoverableException.class)
                    .getResources()
                    .first()
                    .get();
            equivalentContentStore.updateContent(content);
            timer.stop();
        } catch (WriteException e) {
            throw new RecoverableException("update failed for content " + message.getUpdatedResource(), e);
        }
    }

}
