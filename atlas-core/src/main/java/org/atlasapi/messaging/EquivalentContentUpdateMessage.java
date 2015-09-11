package org.atlasapi.messaging;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

public class EquivalentContentUpdateMessage extends AbstractMessage {

    private final Long contentId;

    public EquivalentContentUpdateMessage(
            String messageId,
            Timestamp timestamp,
            Long contentId
    ) {
        super(messageId, timestamp);
        this.contentId = contentId;
    }

    public Long getContentId() {
        return contentId;
    }
}
