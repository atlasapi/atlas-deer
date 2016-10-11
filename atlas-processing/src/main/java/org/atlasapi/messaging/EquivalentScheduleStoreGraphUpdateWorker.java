package org.atlasapi.messaging;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.EquivalentScheduleWriter;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Meter;
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

    private final EquivalentScheduleWriter scheduleWriter;
    private final EquivalenceGraphUpdateResolver graphUpdateResolver;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;

    private EquivalentScheduleStoreGraphUpdateWorker(
            EquivalentScheduleWriter scheduleWriter,
            EquivalenceGraphUpdateResolver graphUpdateResolver,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.graphUpdateResolver = checkNotNull(graphUpdateResolver);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
    }

    public static EquivalentScheduleStoreGraphUpdateWorker create(
            EquivalentScheduleWriter scheduleWriter,
            EquivalenceGraphUpdateResolver graphUpdateResolver,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentScheduleStoreGraphUpdateWorker(
                scheduleWriter, graphUpdateResolver, metricPrefix, metricRegistry
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
                getTimeToProcessInSeconds(message),
                message
        );

        Timer.Context time = executionTimer.time();

        try {
            ImmutableSet<EquivalenceGraph> graphs =
                    graphUpdateResolver.resolve(message.getGraphUpdate());

            scheduleWriter.updateEquivalences(graphs);
        } catch (WriteException e) {
            failureMeter.mark();
            throw new RecoverableException("update failed for " + message.getGraphUpdate(), e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
