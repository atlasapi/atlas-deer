package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import org.atlasapi.content.ItemAndBroadcast;

final class ScheduleBlocksUpdate {

    private final List<ChannelSchedule> updatedBlocks;
    /**
     * Broadcasts which are no longer in the schedule that need to be removed
     */
    private final Set<ItemAndBroadcast> staleEntries;
    /**
     * Broadcasts where content changed, but the broadcast stays in the schedule.
     * We need this in order to be able to mark broadcasts as stale inside the content.
     */
    private final Set<ItemAndBroadcast> staleContent;

    public ScheduleBlocksUpdate(
            List<ChannelSchedule> updatedBlocks,
            Set<ItemAndBroadcast> staleEntries,
            Set<ItemAndBroadcast> staleContent
    ) {
        this.updatedBlocks = checkNotNull(updatedBlocks);
        this.staleEntries = checkNotNull(staleEntries);
        this.staleContent = checkNotNull(staleContent);
    }
    
    public List<ChannelSchedule> getUpdatedBlocks() {
        return this.updatedBlocks;
    }
    
    public Set<ItemAndBroadcast> getStaleEntries() {
        return this.staleEntries;
    }

    public Set<ItemAndBroadcast> getStaleContent() {
        return staleContent;
    }
}
