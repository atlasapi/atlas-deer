package org.atlasapi.eventV2;

public interface EventV2PersistenceWriter {

    void write(EventV2 event, EventV2 previous);
}
