package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.content.ContentRef;

public class EquivalentContentUpdatedMessage extends AbstractMessage {

    private final Long equivalentSetId;
    private final ContentRef contentRef;

    @JsonCreator
    public EquivalentContentUpdatedMessage(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("equivalentSetId") Long equivalentSetId,
            @JsonProperty("contentRef") ContentRef contentRef
    ) {
        super(messageId, timestamp);
        this.equivalentSetId = equivalentSetId;
        this.contentRef = contentRef;
    }

    @JsonProperty("equivalentSetId")
    public Long getEquivalentSetId() {
        return equivalentSetId;
    }

    @JsonProperty("contentRef")
    public ContentRef getContentRef() {
        return contentRef;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("messageId", getMessageId())
                .add("timestamp", getTimestamp())
                .add("equivalentSetId", equivalentSetId)
                .add("contentRef", contentRef)
                .toString();
    }
}
