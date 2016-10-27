package org.atlasapi.messaging;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.atlasapi.content.Content;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;

import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private EquivalentScheduleStoreContentUpdateWorker(
            EquivalentContentStore contentStore,
            EquivalentScheduleWriter scheduleWriter,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.contentStore = checkNotNull(contentStore);
        this.scheduleWriter = checkNotNull(scheduleWriter);

        this.executionTimer = metricRegistry.timer(metricPrefix + "timer.execution");
        this.messageReceivedMeter = metricRegistry.meter(metricPrefix + "meter.received");
        this.failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
        this.latencyTimer = metricRegistry.timer(metricPrefix + "timer.latency");
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
        messageReceivedMeter.mark();

        LOG.debug("Processing message on id: {}, took: PT{}S, message: {}",
                message.getEquivalentSetId(),
                getTimeToProcessInMIllis(message.getTimestamp()) / 1000L,
                message
        );

        Timer.Context time = executionTimer.time();

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

            latencyTimer.update(
                    getTimeToProcessInMIllis(message.getTimestamp()),
                    TimeUnit.MILLISECONDS
            );
        } catch (WriteException e) {
            failureMeter.mark();
            throw new RecoverableException(e);
        } finally {
            time.stop();
        }
    }

    private long getTimeToProcessInMIllis(Timestamp messageTimestamp) {
        return System.currentTimeMillis() - messageTimestamp.millis();
    }
}
