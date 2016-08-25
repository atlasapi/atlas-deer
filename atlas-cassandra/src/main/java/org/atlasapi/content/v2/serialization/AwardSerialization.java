package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Award;


public class AwardSerialization {

    public Award serialize(org.atlasapi.entity.Award award) {
        Award internal = new Award();

        internal.setOutcome(award.getOutcome());
        internal.setTitle(award.getTitle());
        internal.setDescription(award.getDescription());
        internal.setYear(award.getYear());

        return internal;
    }

    public org.atlasapi.entity.Award deserialize(Award internal) {
        org.atlasapi.entity.Award award = new org.atlasapi.entity.Award();

        award.setOutcome(internal.getOutcome());
        award.setTitle(internal.getTitle());
        award.setDescription(internal.getDescription());
        award.setYear(internal.getYear());

        return award;
    }
}
