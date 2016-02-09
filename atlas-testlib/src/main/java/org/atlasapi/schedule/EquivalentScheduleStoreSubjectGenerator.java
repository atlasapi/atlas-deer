package org.atlasapi.schedule;

import org.atlasapi.content.ContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;

public interface EquivalentScheduleStoreSubjectGenerator {

    EquivalentScheduleStore getEquivalentScheduleStore();

    EquivalenceGraphStore getEquivalenceGraphStore();

    ContentStore getContentStore();

}
