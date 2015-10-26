package org.atlasapi.messaging;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.IndexException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentIndexingGraphWorker implements Worker<EquivalenceGraphUpdateMessage> {

    private static final String METRICS_TIMER = "EquivalentContentIndexingGraphWorker";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentIndex contentIndex;
    private final Timer timer;

    public EquivalentContentIndexingGraphWorker(ContentIndex contentIndex, MetricRegistry metricRegistry) {
        this.contentIndex = checkNotNull(contentIndex);
        this.timer = checkNotNull(metricRegistry.timer(METRICS_TIMER));
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        log.debug("Processing message {}", message.toString());
        Timer.Context time = timer.time();
        try {
            for (EquivalenceGraph graph : message.getGraphUpdate().getAllGraphs()) {
                contentIndex.updateCanonicalIds(graph.getId(), graph.getEquivalenceSet());
            }
            time.stop();
        } catch (Exception e) {
            throw new RecoverableException("Failed to update canonical IDs for set" + message.getGraphUpdate().getUpdated().getId(), e);
        }
    }
}