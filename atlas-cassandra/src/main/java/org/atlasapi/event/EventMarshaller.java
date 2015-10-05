package org.atlasapi.event;


import org.atlasapi.entity.Id;

public interface EventMarshaller<M,U> {

    void marshallInto(Id id, M columnBatch, Event event);

    Event unmarshall(U data);

}