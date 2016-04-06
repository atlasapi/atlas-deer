package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Rating;
import org.atlasapi.media.entity.Publisher;

public class RatingSerialization {

    public Rating serialize(org.atlasapi.entity.Rating rating) {
        Rating internal = new Rating();

        internal.setValue(rating.getValue());
        internal.setType(rating.getType());
        internal.setPublisher(rating.getPublisher().key());

        return internal;
    }

    public org.atlasapi.entity.Rating deserialize(Rating rating) {
        return new org.atlasapi.entity.Rating(
                rating.getType(),
                rating.getValue(),
                Publisher.fromKey(rating.getPublisher()).requireValue()
        );
    }
}
