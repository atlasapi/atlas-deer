package org.atlasapi.messaging;

import org.atlasapi.entity.Id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

public class BroadcastRefConfiguration {

    @JsonCreator
    public BroadcastRefConfiguration(
            @JsonProperty("sourceId") String sourceId,
            @JsonProperty("channelId") Id channelId,
            @JsonProperty("transmissionInterval") Interval transmissionInterval) {

    }

}
