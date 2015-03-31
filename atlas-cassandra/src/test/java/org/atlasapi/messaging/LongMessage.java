package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;


public final class LongMessage extends AbstractMessage {

    private Long value;

    public LongMessage(String messageId, Timestamp timestamp, Long value) {
        super(messageId, timestamp);
        this.value = checkNotNull(value, "null value");
    }

    public Long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("%s@%s: %s", getMessageId(), getTimestamp(), value);
    }

}
