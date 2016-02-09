package org.atlasapi.messaging;

import java.util.Map;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

public class EquivalenceGraphConfiguration {

    @JsonCreator
    public EquivalenceGraphConfiguration(
            @JsonProperty("adjacencyList") Map<Id, Adjacents> adjacencyList,
            @JsonProperty("updated") DateTime updated) {

    }

}
