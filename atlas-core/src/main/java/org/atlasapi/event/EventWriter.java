package org.atlasapi.event;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

public interface EventWriter {

    WriteResult<Event, Event> write(Event event) throws WriteException;
}
