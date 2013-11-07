package org.atlasapi.messaging;


/**
 * Message signaling the replay of a given source message.
 */
public class ReplayMessage extends AbstractMessage {

    private final Message original;
    
    public ReplayMessage(String messageId, Long timestamp, String entityId, String entityType, String entitySource, Message original) {
        super(messageId, timestamp, entityId, entityType, entitySource);
        this.original = original;
    }

    public Message getOriginal() {
        return original;
    }

    @Override
    public void dispatchTo(Worker worker) {
        worker.process(this);
    }
}
