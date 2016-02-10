package org.atlasapi.messaging;

import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(builder = EquivalenceGraphUpdate.Builder.class)
public class EquivalenceGraphUpdateConfiguration {

    public static class Builder {

        @JsonCreator
        public Builder(@JsonProperty("updated") EquivalenceGraph updated) {

        }

    }

}
