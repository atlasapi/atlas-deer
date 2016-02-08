package org.atlasapi.messaging;

import org.atlasapi.schedule.ScheduleUpdate;

import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ScheduleUpdateMessageConfiguration {

    @JsonCreator
    public ScheduleUpdateMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("update") ScheduleUpdate scheduleUpdate) {

    }

}
