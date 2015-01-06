package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.Worker;

public class EquivalentContentStoreGraphUpdateWorker implements Worker<EquivalenceGraphUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentContentStore equivalentContentStore;
    private final Timer messageTimer;

    public EquivalentContentStoreGraphUpdateWorker(EquivalentContentStore equivalentContentStore, @Nullable
            MetricRegistry metrics) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer("EquivalentContentStoreGraphUpdateWorker")) : null);
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) {
        try {
            if (messageTimer != null) {
                Timer.Context timer = messageTimer.time();
                equivalentContentStore.updateEquivalences(message.getGraphUpdate());
                timer.stop();
            } else {
                equivalentContentStore.updateEquivalences(message.getGraphUpdate());
            }
        } catch (WriteException e) {
            log.error("update failed for " + message.getGraphUpdate(), e);
        }
    }

}
