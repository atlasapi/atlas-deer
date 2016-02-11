package org.atlasapi.eventV2;

import org.atlasapi.event.Event;

public interface EventV2PersistenceWriter {

    void write(EventV2 event, EventV2 previous);
}
