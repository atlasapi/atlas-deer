package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.UpdateTimes;
import org.atlasapi.content.v2.serialization.setters.IdentifiedWithoutUpdateTimesSetter;
import org.atlasapi.content.v2.serialization.setters.WithUpdateTimesSetter;

public class RestrictionSerialization {

    private final IdentifiedWithoutUpdateTimesSetter identifiedSetter = new IdentifiedWithoutUpdateTimesSetter();
    private final WithUpdateTimesSetter timesSetter = new WithUpdateTimesSetter();

    public Restriction serialize(org.atlasapi.content.Restriction restriction) {
        if (restriction == null) {
            return null;
        }

        Restriction internal = new Restriction();

        identifiedSetter.serialize(internal, restriction);

        internal.setRestricted(restriction.isRestricted());
        internal.setMinimumAge(restriction.getMinimumAge());
        internal.setMessage(restriction.getMessage());
        internal.setAuthority(restriction.getAuthority());
        internal.setRating(restriction.getRating());

        return internal;
    }

    public org.atlasapi.content.Restriction deserialize(
            UpdateTimes updateTimes,
            Restriction internal
    ) {
        if (internal == null) {
            return null;
        }

        org.atlasapi.content.Restriction restriction = new org.atlasapi.content.Restriction();

        identifiedSetter.deserialize(restriction, internal);

        timesSetter.deserialize(restriction, updateTimes);

        restriction.setRestricted(internal.getRestricted());
        restriction.setMinimumAge(internal.getMinimumAge());
        restriction.setMessage(internal.getMessage());
        restriction.setAuthority(internal.getAuthority());
        restriction.setRating(internal.getRating());

        return restriction;
    }

}