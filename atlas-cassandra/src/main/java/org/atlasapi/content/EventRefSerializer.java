package org.atlasapi.content;


import org.atlasapi.entity.Id;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.ContentProtos;

public class EventRefSerializer {

    public ContentProtos.EventRef serialize(EventRef eventRef) {
        ContentProtos.EventRef.Builder ref = ContentProtos.EventRef.newBuilder();
        ref.setId(eventRef.getId().toString());
        ref.setSource(eventRef.getSource().key());
        return ref.build();
    }

    public EventRef deserialize(ContentProtos.EventRef eventRef) {
        return new EventRef(Id.valueOf(eventRef.getId()),
                Publisher.fromKey(eventRef.getSource()).valueOrNull());
    }

}
