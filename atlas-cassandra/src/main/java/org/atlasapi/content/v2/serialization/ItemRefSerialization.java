package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.ClipRef;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.FilmRef;
import org.atlasapi.content.SongRef;
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

        ItemRef internal = new ItemRef();

        Ref ref = new Ref();
        Id id = itemRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }

        Publisher source = itemRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        internal.setSortKey(itemRef.getSortKey());
        internal.setUpdated(toInstant(itemRef.getUpdated()));

        if (itemRef instanceof EpisodeRef) {
            internal.setType("episode");
        } else if (itemRef instanceof FilmRef) {
            internal.setType("film");
        } else if (itemRef instanceof SongRef) {
            internal.setType("song");
        } else if (itemRef instanceof ClipRef) {
            internal.setType("clip");
        } else {
            internal.setType("item");
        }

        return internal;
    }

    public org.atlasapi.content.ItemRef deserialize(Ref ref, ItemRef itemRef) {
        switch (itemRef.getType()) {
        case "episode":
            return new org.atlasapi.content.EpisodeRef(
                    Id.valueOf(ref.getId()),
                    Publisher.fromKey(ref.getSource()).requireValue(),
                    itemRef.getSortKey(),
                    toDateTime(itemRef.getUpdated())
            );
        case "film":
            return new org.atlasapi.content.FilmRef(
                    Id.valueOf(ref.getId()),
                    Publisher.fromKey(ref.getSource()).requireValue(),
                    itemRef.getSortKey(),
                    toDateTime(itemRef.getUpdated())
            );
        case "song":
            return new org.atlasapi.content.SongRef(
                    Id.valueOf(ref.getId()),
                    Publisher.fromKey(ref.getSource()).requireValue(),
                    itemRef.getSortKey(),
                    toDateTime(itemRef.getUpdated())
            );
        case "clip":
            return new org.atlasapi.content.ClipRef(
                    Id.valueOf(ref.getId()),
                    Publisher.fromKey(ref.getSource()).requireValue(),
                    itemRef.getSortKey(),
                    toDateTime(itemRef.getUpdated())
            );
        case "item":
            return new org.atlasapi.content.ItemRef(
                    Id.valueOf(ref.getId()),
                    Publisher.fromKey(ref.getSource()).requireValue(),
                    itemRef.getSortKey(),
                    toDateTime(itemRef.getUpdated())
            );
        default:
            throw new IllegalArgumentException(String.format(
                    "unrecognised itemref type %s",
                    itemRef.getType()
            ));
        }
    }

}