package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Synopses;

public class SynopsesSerialization {

    public Synopses serialize(org.atlasapi.content.Synopses internal) {
        if (internal == null) {
            return null;
        }

        Synopses synopses = new Synopses();

        synopses.setShortDescr(internal.getShortDescription());
        synopses.setMediumDescr(internal.getMediumDescription());
        synopses.setLongDescr(internal.getLongDescription());

        return synopses;
    }

    public org.atlasapi.content.Synopses deserialize(Synopses internal) {
        if (internal == null) {
            return null;
        }

        org.atlasapi.content.Synopses synopses = new org.atlasapi.content.Synopses();

        synopses.setShortDescription(internal.getShortDescr());
        synopses.setMediumDescription(internal.getMediumDescr());
        synopses.setLongDescription(internal.getLongDescr());

        return synopses;
    }

}