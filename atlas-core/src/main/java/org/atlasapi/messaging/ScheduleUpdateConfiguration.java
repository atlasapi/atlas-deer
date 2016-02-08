package org.atlasapi.messaging;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleRef;
import org.atlasapi.schedule.ScheduleUpdate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(builder = ScheduleUpdate.Builder.class)
public class ScheduleUpdateConfiguration {

    public static class Builder {

        @JsonCreator
        public Builder(
                @JsonProperty("source") Publisher source,
                @JsonProperty("schedule") ScheduleRef schedule) {
        }

    }

}
