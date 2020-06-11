package org.atlasapi.messaging;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
    private final Timer latencyTimer;

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
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
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
        long start = System.currentTimeMillis();

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

            scheduleWriter.updateEquivalences(graphs);

            long timeToProcessInMillis = getTimeToProcessInMillis(message.getTimestamp());

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long end = System.currentTimeMillis();
            LOG.info(
                    "Timings - Execution Time (ms): {}, Latency (ms): {}",
                    end - start,
                    timeToProcessInMillis
            );
        } catch (WriteException e) {
            failureMeter.mark();
            throw new RecoverableException("update failed for " + message.getGraphUpdate(), e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
