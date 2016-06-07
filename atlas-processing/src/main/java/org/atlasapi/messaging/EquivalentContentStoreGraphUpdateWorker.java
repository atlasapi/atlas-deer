package org.atlasapi.messaging;

import javax.annotation.Nullable;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentStoreGraphUpdateWorker
        implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentStoreGraphUpdateWorker.class);

    private final EquivalentContentStore equivalentContentStore;
    private final Timer messageTimer;

    public EquivalentContentStoreGraphUpdateWorker(EquivalentContentStore equivalentContentStore,
            @Nullable
            MetricRegistry metrics) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
        this.messageTimer = (metrics != null ? checkNotNull(metrics.timer(
                "EquivalentContentStoreGraphUpdateWorker")) : null);
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Processing message on updated graph: {}, took: PT{}S, "
                            + "created graph(s): {}, deleted graph(s): {}, message: {}",
                    message.getGraphUpdate().getUpdated().getId(),
                    message.getGraphUpdate().getCreated().stream()
                            .map(EquivalenceGraph::getId)
                            .collect(MoreCollectors.toList()),
                    message.getGraphUpdate().getDeleted(),
                    getTimeToProcessInSeconds(message),
                    message
            );
        }

        try {
            if (messageTimer != null) {
                Timer.Context timer = messageTimer.time();
                equivalentContentStore.updateEquivalences(message.getGraphUpdate());
                timer.stop();
            } else {
                equivalentContentStore.updateEquivalences(message.getGraphUpdate());
            }
        } catch (WriteException e) {
            LOG.warn(
                    "Failed to process message on updated graph: {}, created graph(s): {},"
                            + "deleted graph(s): {}, message: {}. Retrying...",
                    message.getGraphUpdate().getUpdated().getId(),
                    message.getGraphUpdate().getCreated().stream()
                            .map(EquivalenceGraph::getId)
                            .collect(MoreCollectors.toList()),
                    message.getGraphUpdate().getDeleted(),
                    message
            );
            throw new RecoverableException("update failed for " + message.getGraphUpdate(), e);
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
