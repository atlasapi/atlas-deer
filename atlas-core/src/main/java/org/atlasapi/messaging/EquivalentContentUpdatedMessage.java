package org.atlasapi.messaging;

import org.atlasapi.content.ContentRef;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Objects;

public class EquivalentContentUpdatedMessage extends AbstractMessage {

    private final Long equivalentSetId;
    private final ContentRef contentRef;

    public EquivalentContentUpdatedMessage(
            String messageId,
            Timestamp timestamp,
            Long equivalentSetId,
            ContentRef contentRef
    ) {
        super(messageId, timestamp);
        this.equivalentSetId = equivalentSetId;
        this.contentRef = contentRef;
    }

    public Long getEquivalentSetId() {
        return equivalentSetId;
    }

    public ContentRef getContentRef() {
        return contentRef;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("messageId", getMessageId())
                .add("timestamp", getTimestamp())
                .add("equivalentSetId", equivalentSetId)
                .add("contentRef", contentRef)
                .toString();
    }
}
