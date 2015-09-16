package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;

public class EquivalentContentUpdatedMessageConfiguration {

    @JsonCreator
    public EquivalentContentUpdatedMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("equivalentSetId") Long equivalentSetId) {
    }
}
