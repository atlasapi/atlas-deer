package org.atlasapi.messaging;

import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.queue.RecoverableException;
import com.metabroadcast.common.queue.Worker;
import org.atlasapi.content.Content;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.util.ImmutableCollectors;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalentScheduleStoreContentUpdateWorker  implements Worker<EquivalentContentUpdatedMessage> {

    private final EquivalentContentStore contentStore;
    private final EquivalentScheduleWriter scheduleWriter;

    public EquivalentScheduleStoreContentUpdateWorker(
            EquivalentContentStore contentStore,
            EquivalentScheduleWriter scheduleWriter
    ) {
        this.contentStore = checkNotNull(contentStore);
        this.scheduleWriter = checkNotNull(scheduleWriter);
    }

    @Override
    public void process(EquivalentContentUpdatedMessage message) throws RecoverableException {
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
                            .collect(ImmutableCollectors.toSet())
            );
        } catch (WriteException e) {
            throw new RecoverableException(e);
        }
    }
}
