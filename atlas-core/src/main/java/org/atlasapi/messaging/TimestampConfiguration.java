package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TimestampConfiguration {

    @JsonCreator
    public TimestampConfiguration(@JsonProperty("millis") Long millis) {
    }

}
