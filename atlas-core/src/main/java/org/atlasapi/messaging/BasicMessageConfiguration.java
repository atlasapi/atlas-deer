package org.atlasapi.messaging;

import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicMessageConfiguration {

    @JsonCreator
    BasicMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp) {
    }

}
