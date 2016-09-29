package org.atlasapi.messaging;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.ScheduleUpdateMessage;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentScheduleStoreScheduleUpdateWorker implements Worker<ScheduleUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentScheduleStoreScheduleUpdateWorker.class);

    private final EquivalentScheduleWriter scheduleWriter;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;

    private EquivalentScheduleStoreScheduleUpdateWorker(
            EquivalentScheduleWriter scheduleWriter,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.scheduleWriter = checkNotNull(scheduleWriter);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
    }

    public static EquivalentScheduleStoreScheduleUpdateWorker create(
            EquivalentScheduleWriter scheduleWriter,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentScheduleStoreScheduleUpdateWorker(
                scheduleWriter, metricPrefix, metricRegistry
        );
    }

    @Override
    public void process(ScheduleUpdateMessage message) throws RecoverableException {
        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getScheduleUpdate().getSchedule().getChannel(),
                getTimeToProcessInSeconds(message),
                message
        );

        Timer.Context time = executionTimer.time();

        try {
            scheduleWriter.updateSchedule(message.getScheduleUpdate());
        } catch (WriteException e) {
            failureMeter.mark();
            throw new RecoverableException("update failed for " + message.getScheduleUpdate(), e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
