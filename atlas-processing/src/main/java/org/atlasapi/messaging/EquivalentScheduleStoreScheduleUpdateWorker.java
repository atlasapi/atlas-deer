package org.atlasapi.messaging;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentScheduleStoreScheduleUpdateWorker implements Worker<ScheduleUpdateMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentScheduleStoreScheduleUpdateWorker.class);

    private final EquivalentScheduleWriter scheduleWriter;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final String publisherExecutionTimerName;
    private final String publisherMeterName;
    private final String publisherLatencyTimerName;

    private final MetricRegistry metricRegistry;

    private EquivalentScheduleStoreScheduleUpdateWorker(
            EquivalentScheduleWriter scheduleWriter,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.scheduleWriter = checkNotNull(scheduleWriter);

        this.metricRegistry = metricRegistry;

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
        this.publisherMeterName = metricPrefix + "source.%s.meter.received";
        this.publisherExecutionTimerName = metricPrefix + "source.%s.timer.execution";
        this.publisherLatencyTimerName = metricPrefix + "source.%s.timer.latency";
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
        long start = System.currentTimeMillis();

        messageReceivedMeter.mark();

        LOG.debug("Processing message on id {}, took: PT{}S, message: {}",
                message.getScheduleUpdate().getSchedule().getChannel(),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                message
        );

        String metricSourceName = message.getScheduleUpdate().getSource().key().replace('.', '_');

        metricRegistry.meter(String.format(publisherMeterName, metricSourceName)).mark();

        Timer publisherExecutionTimer = metricRegistry.timer(String.format(publisherExecutionTimerName, metricSourceName));
        Timer publisherLatencyTimer = metricRegistry.timer(String.format(publisherLatencyTimerName, metricSourceName));

        Timer.Context time = executionTimer.time();
        Timer.Context publisherTime = publisherExecutionTimer.time();

        try {
            scheduleWriter.updateSchedule(message.getScheduleUpdate());

            long timeToProcessInMillis = getTimeToProcessInMillis(message.getTimestamp());

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);
            publisherLatencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long end = System.currentTimeMillis();
            LOG.info(
                    "Timings - Source: {}, Execution Time (ms): {}, Latency (ms): {}",
                    message.getScheduleUpdate().getSource().key(),
                    end - start,
                    timeToProcessInMillis
            );
        } catch (WriteException e) {
            failureMeter.mark();
            throw new RecoverableException("update failed for " + message.getScheduleUpdate(), e);
        } finally {
            time.stop();
            publisherTime.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
