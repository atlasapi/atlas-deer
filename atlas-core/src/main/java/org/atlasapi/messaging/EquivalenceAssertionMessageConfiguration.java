package org.atlasapi.messaging;

import org.atlasapi.entity.ResourceRef;

import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(builder = EquivalenceAssertionMessage.Builder.class)
public class EquivalenceAssertionMessageConfiguration {

    public static class Builder {

        @JsonCreator
        public Builder(
                @JsonProperty("messageId") String messageId,
                @JsonProperty("timestamp") Timestamp timestamp,
                @JsonProperty("subject") ResourceRef subject) {
        }

    }

}
