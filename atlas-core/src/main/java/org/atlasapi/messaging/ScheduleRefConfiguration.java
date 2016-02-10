package org.atlasapi.messaging;

import org.atlasapi.content.BroadcastRef;
import org.atlasapi.entity.Id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.joda.time.Interval;

public class ScheduleRefConfiguration {

    @JsonCreator
    public ScheduleRefConfiguration(
            @JsonProperty("channel") Id channel,
            @JsonProperty("interval") Interval interval,
            @JsonProperty("entries") ImmutableList<Entry> entries) {

    }

    public static class Entry {

        @JsonCreator
        public Entry(@JsonProperty("item") Id item,
                @JsonProperty("broadcast") BroadcastRef broadcast) {
        }

    }

}
