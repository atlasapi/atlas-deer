package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

public abstract class AdjacentsConfiguration {

    @JsonCreator
    public AdjacentsConfiguration(
            @JsonProperty("subject") ResourceRef subject,
            @JsonProperty("created") DateTime created,
            @JsonProperty("efferent") Set<ResourceRef> efferent,
            @JsonProperty("afferent") Set<ResourceRef> afferent) {

    }

    @JsonProperty("efferent")
    public abstract ImmutableSet<ResourceRef> getEfferent();

    @JsonProperty("afferent")
    public abstract ImmutableSet<ResourceRef> getAfferent();
}
