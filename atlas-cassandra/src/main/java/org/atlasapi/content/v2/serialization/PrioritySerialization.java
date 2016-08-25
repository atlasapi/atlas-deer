package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.content.v2.model.udt.Priority;

public class PrioritySerialization {

    public Priority serialize(org.atlasapi.content.Priority priority) {
        if (priority == null) {
            return null;
        }

        Priority internal = new Priority();

        internal.setPriority(priority.getPriority());
        PriorityScoreReasons reasons = priority.getReasons();
        if (reasons != null) {
            internal.setPositive(reasons.getPositive());
            internal.setNegative(reasons.getNegative());
        }

        return internal;
    }

    public org.atlasapi.content.Priority deserialize(Priority internal) {
        if (internal == null) {
            return null;
        }

        return new org.atlasapi.content.Priority(
                internal.getPriority(),
                new PriorityScoreReasons(
                        internal.getPositive(), internal.getNegative())
        );
    }
}