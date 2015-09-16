package org.atlasapi.messaging;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

public class EquivalenceUpdatedMessage extends AbstractMessage {

    private final Long equivalentSetId;

    public EquivalenceUpdatedMessage(
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
