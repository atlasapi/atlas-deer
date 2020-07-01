package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver;

public class IdConfiguration {

    @JsonCreator
    public IdConfiguration(@JsonProperty("longValue") Long longValue) {
    }

    @JsonTypeResolver(CustomTypeResolverBuilder.AllowPrimitives.class)
    public void longValue() {
    }

}
