package org.atlasapi.messaging;

import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.MessageStore;

public class MessageLogger extends BaseWorker {

    private final MessageStore store;

    public MessageLogger(MessageStore store) {
        this.store = store;
    }

    @Override
    public void process(EntityUpdatedMessage message) {
        store.add(message);
    }
}