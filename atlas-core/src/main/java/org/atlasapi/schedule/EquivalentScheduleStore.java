package org.atlasapi.schedule;

/**
 * <p>A store for {@link EquivalentSchedule}s, with a full equivalence set of {@link
 * org.atlasapi.content.Item Item}s for each {@link org.atlasapi.content.Broadcast Broadcast} in the
 * schedule.</p>
 */
public interface EquivalentScheduleStore extends EquivalentScheduleResolver,
        EquivalentScheduleWriter {

}
