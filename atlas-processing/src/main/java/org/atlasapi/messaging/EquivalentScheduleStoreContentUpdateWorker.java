package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.stream.Stream;

import org.atlasapi.content.Content;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.util.ImmutableCollectors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;

public class EquivalentScheduleStoreContentUpdateWorker  implements Worker<EquivalentContentUpdatedMessage> {

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
                            .collect(ImmutableCollectors.toSet())
            );
            timer.stop();
        } catch (WriteException e) {
            throw new RecoverableException(e);
        }
    }
}
