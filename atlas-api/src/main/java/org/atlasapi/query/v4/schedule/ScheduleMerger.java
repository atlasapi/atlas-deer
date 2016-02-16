package org.atlasapi.query.v4.schedule;

import org.atlasapi.schedule.ChannelSchedule;

interface ScheduleMerger {
    ChannelSchedule merge(ChannelSchedule original, ChannelSchedule override);
}
