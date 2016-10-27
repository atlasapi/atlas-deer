package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.ResourceRef;

public class RefSerialization {

    public Ref serialize(ResourceRef ref) {
        Ref internal = new Ref();

        internal.setId(ref.getId().longValue());
        internal.setSource(ref.getSource().key());

        return internal;
    }
}
