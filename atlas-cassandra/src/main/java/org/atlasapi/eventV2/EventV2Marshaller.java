package org.atlasapi.eventV2;

import org.atlasapi.entity.Id;
import org.atlasapi.eventV2.EventV2;

public interface EventV2Marshaller<M, U> {

    void marshallInto(Id id, M columnBatch, EventV2 event);

    EventV2 unmarshall(U data);
}
