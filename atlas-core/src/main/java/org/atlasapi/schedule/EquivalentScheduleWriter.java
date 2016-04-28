package org.atlasapi.schedule;

import org.atlasapi.content.Item;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;

import com.google.common.collect.ImmutableSet;

/**
 * Maintains an equivalent schedule that can be resolved through an {@link
 * EquivalentScheduleResolver}.
 */
public interface EquivalentScheduleWriter {

    void updateSchedule(ScheduleUpdate update) throws WriteException;

    void updateEquivalences(ImmutableSet<EquivalenceGraph> graphs) throws WriteException;

    void updateContent(Iterable<Item> content) throws WriteException;

}
