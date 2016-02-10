package org.atlasapi.messaging;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResourceRefConfiguration {

    @JsonCreator
    ResourceRefConfiguration(
            @JsonProperty("id") Id id,
            @JsonProperty("source") Publisher source) {
    }

}
