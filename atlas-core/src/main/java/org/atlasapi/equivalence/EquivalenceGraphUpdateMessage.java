package org.atlasapi.equivalence;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EquivalenceGraphUpdateMessage extends AbstractMessage {

    private final EquivalenceGraphUpdate update;

    @JsonCreator
    public EquivalenceGraphUpdateMessage(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("update") EquivalenceGraphUpdate update
    ) {
        super(messageId, timestamp);
        this.update = update;
    }

    @JsonProperty("update")
    public EquivalenceGraphUpdate getGraphUpdate() {
        return update;
    }

}
