package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IdConfiguration {

    @JsonCreator
    public IdConfiguration(@JsonProperty("longValue") Long longValue) {
    }

}
