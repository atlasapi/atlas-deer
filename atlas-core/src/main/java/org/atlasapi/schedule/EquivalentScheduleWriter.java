package org.atlasapi.schedule;

import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;

/**
 * Maintains an equivalent schedule that can be resolved through an {@link
 * EquivalentScheduleResolver}.
 */
public interface EquivalentScheduleWriter {

    /**
     * <p>Updates the schedule and removes stale schedule entries according to a {@link
     * ScheduleUpdate} such that it can be retrieved through {@link #resolveSchedules}.</p>
     *
     * @param update - the update to apply
     * @throws WriteException - if there is an error during updating the schedule.
     */
    void updateSchedule(ScheduleUpdate update) throws WriteException;

    /**
     * <p>Update equivalences for particular items according to an {@link EquivalenceUpdate}.</p>
     *
     * @param update - the update to apply.
     * @throws WriteException - if there is an error during updating the schedule.
     */
    void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException;

    /**
     * Updates content in equivalent schedule store.
     *
     * @param content - content in the equivalent set
     * @throws WriteException
     */
    void updateContent(Iterable<Item> content) throws WriteException;

}
