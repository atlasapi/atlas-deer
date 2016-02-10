package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

public class AdjacentsConfiguration {

    @JsonCreator
    public AdjacentsConfiguration(
            @JsonProperty("subject") ResourceRef subject,
            @JsonProperty("created") DateTime created,
            @JsonProperty("efferent") Set<ResourceRef> efferent,
            @JsonProperty("afferent") Set<ResourceRef> afferent) {

    }

}
