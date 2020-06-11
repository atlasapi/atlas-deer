package org.atlasapi.messaging;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.content.Content;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentScheduleStoreContentUpdateWorker
        implements Worker<EquivalentContentUpdatedMessage> {

    private static final Logger LOG =
            LoggerFactory.getLogger(EquivalentScheduleStoreContentUpdateWorker.class);

    private final EquivalentContentStore contentStore;
    private final EquivalentScheduleWriter scheduleWriter;

    private final Timer executionTimer;
    private final Meter messageReceivedMeter;
    private final Meter failureMeter;
    private final Timer latencyTimer;
    private final String publisherExecutionTimerName;
    private final String publisherMeterName;
    private final String publisherLatencyTimerName;

    private final MetricRegistry metricRegistry;


    private EquivalentScheduleStoreContentUpdateWorker(
            EquivalentContentStore contentStore,
            EquivalentScheduleWriter scheduleWriter,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.contentStore = checkNotNull(contentStore);
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

    public static EquivalentScheduleStoreContentUpdateWorker create(
            EquivalentContentStore contentStore,
            EquivalentScheduleWriter scheduleWriter,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new EquivalentScheduleStoreContentUpdateWorker(
                contentStore,
                scheduleWriter,
                metricPrefix,
                metricRegistry
        );
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
        long start = System.currentTimeMillis();

        messageReceivedMeter.mark();

        LOG.debug("Processing message on id: {}, took: PT{}S, message: {}",
                message.getEquivalentSetId(),
                getTimeToProcessInMillis(message.getTimestamp()) / 1000L,
                message
        );

        String metricSourceName = message.getContentRef().getSource().key().replace('.', '_');

        metricRegistry.meter(String.format(publisherMeterName, metricSourceName)).mark();

        Timer publisherExecutionTimer = metricRegistry.timer(String.format(publisherExecutionTimerName, metricSourceName));
        Timer publisherLatencyTimer = metricRegistry.timer(String.format(publisherLatencyTimerName, metricSourceName));

        Timer.Context time = executionTimer.time();
        Timer.Context publisherTime = publisherExecutionTimer.time();

        Set<Content> content = Futures.get(
                contentStore.resolveEquivalentSet(message.getEquivalentSetId()),
                RecoverableException.class
        );
        try {
            scheduleWriter.updateContent(
                    content.stream()
                            .flatMap(c -> {
                                if (c instanceof Item) {
                                    return Stream.of((Item) c);
                                } else {
                                    return Stream.empty();
                                }
                            })
                            .collect(MoreCollectors.toImmutableSet())
            );

            long timeToProcessInMillis = getTimeToProcessInMillis(message.getTimestamp());

            latencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);
            publisherLatencyTimer.update(timeToProcessInMillis, TimeUnit.MILLISECONDS);

            long end = System.currentTimeMillis();
            LOG.info(
                    "Timings - Source: {}, Execution Time (ms): {}, Latency (ms): {}",
                    message.getContentRef().getSource().key(),
                    end - start,
                    timeToProcessInMillis
            );
        } catch (WriteException e) {
            failureMeter.mark();
            throw new RecoverableException(e);
        } finally {
            time.stop();
            publisherTime.stop();
        }
    }

    private long getTimeToProcessInMillis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
