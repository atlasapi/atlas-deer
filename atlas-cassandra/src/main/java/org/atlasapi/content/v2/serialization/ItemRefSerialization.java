package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;
import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class ItemRefSerialization {

    public ItemRef serialize(org.atlasapi.content.ItemRef itemRef) {
        if (itemRef == null) {
            return null;
        }

        ItemRef internal =
                new ItemRef();

        Ref ref = new Ref();
        Id id = itemRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }

        Publisher source = itemRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        internal.setRef(ref);
        internal.setSortKey(itemRef.getSortKey());
        internal.setUpdated(toInstant(itemRef.getUpdated()));

        return internal;
    }

    public org.atlasapi.content.ItemRef deserialize(ItemRef itemRef) {
        return new org.atlasapi.content.ItemRef(
                Id.valueOf(itemRef.getRef().getId()),
                Publisher.fromKey(itemRef.getRef().getSource()).requireValue(),
                itemRef.getSortKey(),
                toDateTime(itemRef.getUpdated())
        );
    }

}