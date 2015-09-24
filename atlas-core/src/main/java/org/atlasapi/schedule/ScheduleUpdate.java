package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.BroadcastRef;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public final class ScheduleUpdate {
    
    public static final class Builder {

        private final Publisher source;
        private final ScheduleRef schedule;
        private ImmutableSet<BroadcastRef> staleBroadcasts = ImmutableSet.of(); 
        
        public Builder(Publisher source, ScheduleRef schedule) {
            this.source = checkNotNull(source);
            this.schedule = checkNotNull(schedule);
        }
        
        public Builder withStaleBroadcasts(Iterable<BroadcastRef> staleBroadcasts) {
            this.staleBroadcasts = ImmutableSet.copyOf(staleBroadcasts);
            return this;
        }
        
        public ScheduleUpdate build(){
            return new ScheduleUpdate(source, schedule, staleBroadcasts);
        }
            
    }

    private final Publisher source;
    private final ScheduleRef schedule;
    private final ImmutableSet<BroadcastRef> staleBroadcasts;
    
    public ScheduleUpdate(Publisher source, ScheduleRef schedule, Iterable<BroadcastRef> staleBroadcasts) {
        this.source = checkNotNull(source);
        this.schedule = checkNotNull(schedule);
        this.staleBroadcasts = ImmutableSet.copyOf(staleBroadcasts);
    }

    public ScheduleRef getSchedule() {
        return schedule;
    }

    public Set<BroadcastRef> getStaleBroadcasts() {
        return staleBroadcasts;
    }

    public Publisher getSource() {
        return source;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("source", source)
                .add("schedule", schedule)
                .add("stale", staleBroadcasts)
                .toString();
    }
    
}
