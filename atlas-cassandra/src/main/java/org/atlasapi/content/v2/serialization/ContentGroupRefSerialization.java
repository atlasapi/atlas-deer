package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.ContentGroupRef;
import org.atlasapi.entity.Id;

public class ContentGroupRefSerialization {

    public ContentGroupRef serialize(org.atlasapi.content.ContentGroupRef contentGroupRef) {
        if (contentGroupRef == null) {
            return null;
        }
        ContentGroupRef internal = new ContentGroupRef();

        Id id = contentGroupRef.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setUri(contentGroupRef.getUri());

        return internal;
    }

    public org.atlasapi.content.ContentGroupRef deserialize(ContentGroupRef ref) {
        if (ref == null) {
            return null;
        }
        return new org.atlasapi.content.ContentGroupRef(Id.valueOf(ref.getId()), ref.getUri());
    }
}