package org.atlasapi.equivalence;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

public class EquivalenceGraphUpdateMessage extends AbstractMessage {

    private final EquivalenceGraphUpdate update;

    public EquivalenceGraphUpdateMessage(String messageId, Timestamp timestamp,
            EquivalenceGraphUpdate updatedGraphs) {
        super(messageId, timestamp);
        this.update = updatedGraphs;
    }

    public EquivalenceGraphUpdate getGraphUpdate() {
        return update;
    }

}
