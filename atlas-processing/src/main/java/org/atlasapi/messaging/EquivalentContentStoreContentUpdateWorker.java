package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.Worker;

public class EquivalentContentStoreContentUpdateWorker implements Worker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentContentStore equivalentContentStore;
    @Nullable private final Timer messageTimer;

    public EquivalentContentStoreContentUpdateWorker(EquivalentContentStore equivalentContentStore, @Nullable
            MetricRegistry metrics) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("EquivalentContentStoreContentUpdateWorker")) : null);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        try {
            if (messageTimer != null) {
                Timer.Context timer = messageTimer.time();
                equivalentContentStore.updateContent(message.getUpdatedResource());
                timer.stop();
            } else {
                equivalentContentStore.updateContent(message.getUpdatedResource());
            }
        } catch (WriteException e) {
            log.error("update failed for content " + message.getUpdatedResource(), e);
        }
    }

}
