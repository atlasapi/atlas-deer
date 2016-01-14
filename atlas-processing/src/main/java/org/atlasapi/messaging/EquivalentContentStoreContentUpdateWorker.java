package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class EquivalentContentStoreContentUpdateWorker implements Worker<ResourceUpdatedMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentStoreContentUpdateWorker.class);

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
        LOG.debug("Processing message on id {}, message: {}",
                message.getUpdatedResource().getId(), message);

        try {
            Timer.Context timer = messageTimer.time();
            equivalentContentStore.updateContent(message.getUpdatedResource().getId());
            timer.stop();
        } catch (WriteException e) {
            throw new RecoverableException("update failed for content " + message.getUpdatedResource(), e);
        }
    }

}
