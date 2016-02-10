package org.atlasapi.messaging;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

public class ItemRefConfiguration {

    @JsonCreator
    ItemRefConfiguration(
            @JsonProperty("id") Id id,
            @JsonProperty("source") Publisher source,
            @JsonProperty("sortKey") String sortKey,
            @JsonProperty("updated") DateTime updated) {
    }

}
