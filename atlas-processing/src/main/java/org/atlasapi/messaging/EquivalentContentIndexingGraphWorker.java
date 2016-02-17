package org.atlasapi.messaging;

import org.atlasapi.content.ContentIndex;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentIndexingGraphWorker implements Worker<EquivalenceGraphUpdateMessage> {

    private static final String METRICS_TIMER = "EquivalentContentIndexingGraphWorker";

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentIndexingContentWorker.class);

    private final ContentIndex contentIndex;
    private final Timer timer;

    public EquivalentContentIndexingGraphWorker(ContentIndex contentIndex,
            MetricRegistry metricRegistry) {
        this.contentIndex = checkNotNull(contentIndex);
        this.timer = checkNotNull(metricRegistry.timer(METRICS_TIMER));
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        LOG.debug(
                "Processing message on ids {}, took: PT{}S, message: {}",
                message.getGraphUpdate().getAllGraphs().stream()
                        .map(EquivalenceGraph::getId)
                        .collect(ImmutableCollectors.toList()),
                getTimeToProcessInSeconds(message),
                message
        );

        Timer.Context time = timer.time();
        try {
            for (EquivalenceGraph graph : message.getGraphUpdate().getAllGraphs()) {
                contentIndex.updateCanonicalIds(graph.getId(), graph.getEquivalenceSet());
            }
            time.stop();
        } catch (Exception e) {
            throw new RecoverableException("Failed to update canonical IDs for set"
                    + message.getGraphUpdate().getUpdated().getId(), e);
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}