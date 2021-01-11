package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.entity.ResourceRef;

/**
 * Message signaling that a given entity has been created or updated.
 */
public class ResourceUpdatedMessage extends AbstractMessage {

    private ResourceRef updatedResource;

    @JsonCreator
    public ResourceUpdatedMessage(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("updatedResource") ResourceRef updatedResource
    ) {
        super(messageId, timestamp);
        this.updatedResource = updatedResource;
    }

    @JsonProperty("updatedResource")
    public ResourceRef getUpdatedResource() {
        return updatedResource;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("updatedResource", updatedResource)
                .add("timestamp", getTimestamp().toDateTimeUTC().toString())
                .add("id", getMessageId())
                .toString();
    }
}
