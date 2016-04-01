package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Description;

public class DescriptionSerialization {

    public Description serialize(org.atlasapi.content.Description description) {
        if (description == null) {
            return null;
        }

        Description internal = new Description();

        internal.setTitle(description.getTitle());
        internal.setImage(description.getImage());
        internal.setSynopsis(description.getSynopsis());
        internal.setThumbnail(description.getThumbnail());

        return internal;
    }

    public org.atlasapi.content.Description deserialize(Description internal) {
        if (internal == null) {
            return null;
        }

        return new org.atlasapi.content.Description(
                internal.getTitle(),
                internal.getSynopsis(),
                internal.getImage(),
                internal.getThumbnail()
        );
    }
}