package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.Id;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.entity.Publisher;

public class EventRefSerialization {

    public Ref serialize(EventRef eventRef) {
        if (eventRef == null) {
            return null;
        }

        Ref ref = new Ref();

        Id id = eventRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }
        Publisher source = eventRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        return ref;
    }

    public EventRef deserialize(Ref ref) {
        if (ref == null) {
            return null;
        }
        return new EventRef(
                Id.valueOf(ref.getId()),
                Publisher.fromKey(ref.getSource()).requireValue()
        );
    }
}
