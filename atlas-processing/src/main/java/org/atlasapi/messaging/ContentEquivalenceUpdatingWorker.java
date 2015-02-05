package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentEquivalenceUpdatingWorker implements Worker<EquivalenceAssertionMessage> {

    private final Logger log = LoggerFactory.getLogger(ContentEquivalenceUpdatingWorker.class);

    private final EquivalenceGraphStore graphStore;
    @Nullable private final Timer messageTimer;

    public ContentEquivalenceUpdatingWorker(EquivalenceGraphStore graphStore, @Nullable
    MetricRegistry metrics) {
        this.graphStore = checkNotNull(graphStore);
        this.messageTimer = (metrics != null ?
                             checkNotNull(metrics.timer("ContentEquivalenceUpdatingWorker")) :
                             null);
    }

    @Override
    public void process(EquivalenceAssertionMessage message) {
        try {
            if (messageTimer != null) {
                Timer.Context timer = messageTimer.time();
                graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(),
                        message.getPublishers());
                timer.stop();
            } else {
                graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(),
                        message.getPublishers());
            }
        } catch (Exception e) {
            log.warn("Error while processing EquivalenceAssertionMessage {}", message.toString(), e);
        }
    }

}
