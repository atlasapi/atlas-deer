package org.atlasapi.messaging;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

public class EquivalentContentUpdatedMessage extends AbstractMessage {

    private final Long equivalentSetId;

    public EquivalentContentUpdatedMessage(
            String messageId,
            Timestamp timestamp,
            Long equivalentSetId
    ) {
        super(messageId, timestamp);
        this.equivalentSetId = equivalentSetId;
    }

    public Long getEquivalentSetId() {
        return equivalentSetId;
    }
}
