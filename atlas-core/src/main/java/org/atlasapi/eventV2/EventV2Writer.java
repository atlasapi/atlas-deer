package org.atlasapi.eventV2;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;

public interface EventV2Writer {

    WriteResult<EventV2, EventV2> write(EventV2 event) throws WriteException;

}
