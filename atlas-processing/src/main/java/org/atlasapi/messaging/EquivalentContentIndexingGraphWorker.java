package org.atlasapi.messaging;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.ContentIndex;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentContentIndexingGraphWorker implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentContentIndexingContentWorker.class);

    private final ContentIndex contentIndex;
    private final EquivalenceGraphUpdateResolver graphUpdateResolver;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;

    private EquivalentContentIndexingGraphWorker(
            ContentIndex contentIndex,
            EquivalenceGraphUpdateResolver graphUpdateResolver,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.contentIndex = checkNotNull(contentIndex);
        this.graphUpdateResolver = checkNotNull(graphUpdateResolver);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
    }

    public static EquivalentContentIndexingGraphWorker create(
            ContentIndex contentIndex,
            EquivalenceGraphUpdateResolver graphUpdateResolver,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentContentIndexingGraphWorker(
                contentIndex, graphUpdateResolver, metricPrefix, metricRegistry
        );
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        LOG.debug(
                "Processing message on ids {}, took: PT{}S, message: {}",
                message.getGraphUpdate().getAllGraphs().stream()
                        .map(EquivalenceGraph::getId)
                        .collect(MoreCollectors.toImmutableList()),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                message
        );

        Timer.Context time = executionTimer.time();

        try {
            ImmutableSet<EquivalenceGraph> graphs =
                    graphUpdateResolver.resolve(message.getGraphUpdate());

            for (EquivalenceGraph graph : graphs) {
                contentIndex.updateCanonicalIds(graph.getId(), graph.getEquivalenceSet());
            }

            latencyTimer.update(
                    getTimeToProcessInMillis(message.getTimestamp()),
                    TimeUnit.MILLISECONDS
            );
        } catch (Exception e) {
            failureMeter.mark();
            throw new RecoverableException("Failed to update canonical IDs for set"
                    + message.getGraphUpdate().getUpdated().getId(), e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
