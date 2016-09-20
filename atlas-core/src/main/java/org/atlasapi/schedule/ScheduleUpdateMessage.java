package org.atlasapi.schedule;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleUpdateMessage extends AbstractMessage {

    private final ScheduleUpdate update;

    @JsonCreator
    public ScheduleUpdateMessage(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("update") ScheduleUpdate update
    ) {
        super(messageId, timestamp);
        this.update = checkNotNull(update);
    }

    @JsonProperty("update")
    public ScheduleUpdate getScheduleUpdate() {
        return update;
    }

}
