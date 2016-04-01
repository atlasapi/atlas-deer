package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;

public class EquivalenceRefSerialization {

    public Ref serialize(EquivalenceRef equivalenceRef) {
        if (equivalenceRef == null) {
            return null;
        }
        Ref internal = new Ref();

        Id id = equivalenceRef.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        Publisher source = equivalenceRef.getSource();
        if (source != null) {
            internal.setSource(source.key());
        }

        return internal;
    }

    public EquivalenceRef deserialize(Ref ref) {
        return new EquivalenceRef(
                Id.valueOf(ref.getId()),
                Publisher.fromKey(ref.getSource()).requireValue()
        );
    }
}
