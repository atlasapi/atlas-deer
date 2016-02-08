package org.atlasapi.messaging;

import org.atlasapi.entity.ResourceRef;

import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class ResourceUpdatedMessageConfiguration {

    @JsonCreator
    ResourceUpdatedMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("updatedResource") ResourceRef updatedResource) {

    }

}
