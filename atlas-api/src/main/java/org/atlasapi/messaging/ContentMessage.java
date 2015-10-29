package org.atlasapi.messaging;

import org.atlasapi.content.Content;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.time.Timestamp;

public class ContentMessage implements Message {
    private final String id;
    private final Timestamp timestamp;
    private final Content content;

    @JsonCreator
    public ContentMessage(
            @JsonProperty("messageId") String id,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("content") Content content
    ) {
        this.id = id;
        this.timestamp = timestamp;
        this.content = content;
    }

    @Override
    public String getMessageId() {
        return id;
    }

    @Override
    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Content getContent() {
        return content;
    }
}
