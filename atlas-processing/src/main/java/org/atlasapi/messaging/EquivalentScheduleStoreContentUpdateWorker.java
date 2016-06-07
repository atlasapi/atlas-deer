package org.atlasapi.messaging;

import java.util.Set;
import java.util.stream.Stream;

import org.atlasapi.content.Content;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.stream.MoreCollectors;

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
    private final Timer messageTimer;

    public EquivalentScheduleStoreContentUpdateWorker(
            EquivalentContentStore contentStore,
            EquivalentScheduleWriter scheduleWriter,
            MetricRegistry metrics
    ) {
        this.contentStore = checkNotNull(contentStore);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.messageTimer = checkNotNull(metrics.timer("EquivalentScheduleStoreContentUpdateWorker"));
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
        LOG.debug("Processing message on id: {}, took: PT{}S, message: {}",
                message.getEquivalentSetId(),
                getTimeToProcessInSeconds(message),
                message
        );

        Set<Content> content = Futures.get(
                contentStore.resolveEquivalentSet(message.getEquivalentSetId()),
                RecoverableException.class
        );
        try {
            Timer.Context timer = messageTimer.time();
            scheduleWriter.updateContent(
                    content.stream()
                            .flatMap(c -> {
                                if (c instanceof Item) {
                                    return Stream.of((Item) c);
                                } else {
                                    return Stream.empty();
                                }
                            })
                            .collect(MoreCollectors.toSet())
            );
            timer.stop();
        } catch (WriteException e) {
            throw new RecoverableException(e);
        }
    }

    private long getTimeToProcessInSeconds(AbstractMessage message) {
        return (System.currentTimeMillis() - message.getTimestamp().millis()) / 1000L;
    }
}
