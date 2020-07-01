package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver;

public class TimestampConfiguration {

    @JsonCreator
    public TimestampConfiguration(@JsonProperty("millis") Long millis) {
    }

    @JsonTypeResolver(CustomTypeResolverBuilder.AllowPrimitives.class)
    public void millis() {
    }

}
