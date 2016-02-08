package org.atlasapi.messaging;

import org.atlasapi.content.ContentRef;

import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EquivalentContentUpdatedMessageConfiguration {

    @JsonCreator
    public EquivalentContentUpdatedMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("equivalentSetId") Long equivalentSetId,
            @JsonProperty("contentRef") ContentRef contentRef) {
    }
}
