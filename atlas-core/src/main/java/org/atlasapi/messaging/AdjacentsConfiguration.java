package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

/**
 * For legacy reasons we need to refer to outgoing edges as efferent and incoming edges as
 * afferent. This is because {@link org.atlasapi.equivalence.EquivalenceGraph.Adjacents} used to
 * use those names and since it is serialised into / deserialised from JSON there could be messages
 * that still use those names.
 */
public abstract class AdjacentsConfiguration {

    @JsonCreator
    public AdjacentsConfiguration(
            @JsonProperty("subject") ResourceRef subject,
            @JsonProperty("created") DateTime created,
            @JsonProperty("efferent") Set<ResourceRef> outgoingEdges,
            @JsonProperty("afferent") Set<ResourceRef> incomingEdges) {

    }

    @JsonProperty("efferent")
    public abstract ImmutableSet<ResourceRef> getOutgoingEdges();

    @JsonProperty("afferent")
    public abstract ImmutableSet<ResourceRef> getIncomingEdges();
}
