package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentRef;

public class SegmentRefSerialization {

    public Ref serialize(SegmentRef segmentRef) {
        if (segmentRef == null) {
            return null;
        }
        Ref internal = new Ref();

        Id id = segmentRef.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }

        Publisher source = segmentRef.getSource();
        if (source != null) {
            internal.setSource(source.key());
        }

        return internal;
    }

    public SegmentRef deserialize(Ref ref) {
        if (ref == null) {
            return null;
        }
        return new SegmentRef(
                Id.valueOf(ref.getId()),
                Publisher.fromKey(ref.getSource()).requireValue()
        );
    }
}