package org.atlasapi;

import org.atlasapi.content.ContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.eventV2.EventV2Store;
import org.atlasapi.organisation.OrganisationStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.segment.SegmentStore;
import org.atlasapi.topic.TopicStore;

public interface PersistenceModule {

    ContentStore contentStore();

    TopicStore topicStore();

    ScheduleStore scheduleStore();

    ScheduleStore v2ScheduleStore();

    SegmentStore segmentStore();

    EquivalenceGraphStore contentEquivalenceGraphStore();

    EquivalentScheduleStore equivalentScheduleStore();

    EventV2Store eventStore();

    OrganisationStore organisationStore();
}
