package org.atlasapi.messaging;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentScheduleStoreGraphUpdateWorker
        implements Worker<EquivalenceGraphUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentScheduleStoreGraphUpdateWorker.class);

    private static final String METRICS_TIMER = "EquivalentScheduleStoreGraphUpdateWorker";

    private final EquivalentScheduleWriter scheduleWriter;
    private final Timer messageTimer;

    private final EquivalenceGraphUpdateResolver graphUpdateResolver;

    private EquivalentScheduleStoreGraphUpdateWorker(
            EquivalentScheduleWriter scheduleWriter,
            MetricRegistry metrics,
            EquivalenceGraphUpdateResolver graphUpdateResolver
    ) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.messageTimer = checkNotNull(metrics.timer(METRICS_TIMER));
        this.graphUpdateResolver = checkNotNull(graphUpdateResolver);
    }

    public static EquivalentScheduleStoreGraphUpdateWorker create(
            EquivalentScheduleWriter scheduleWriter,
            MetricRegistry metrics,
            EquivalenceGraphUpdateResolver graphUpdateResolver
    ) {
        return new EquivalentScheduleStoreGraphUpdateWorker(
                scheduleWriter, metrics, graphUpdateResolver
        );
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

        try {
            Timer.Context timer = messageTimer.time();

            ImmutableSet<EquivalenceGraph> graphs =
                    graphUpdateResolver.resolve(message.getGraphUpdate());

            scheduleWriter.updateEquivalences(graphs);

            timer.stop();
        } catch (WriteException e) {
            throw new RecoverableException("update failed for " + message.getGraphUpdate(), e);
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
