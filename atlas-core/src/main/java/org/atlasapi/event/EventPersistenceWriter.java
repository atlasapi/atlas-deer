package org.atlasapi.event;

public interface EventPersistenceWriter {

    void write(Event event, Event previous);
}
