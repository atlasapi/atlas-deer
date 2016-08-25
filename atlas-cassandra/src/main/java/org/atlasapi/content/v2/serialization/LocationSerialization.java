package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.content.v2.model.udt.Location;
import org.atlasapi.content.v2.serialization.setters.IdentifiedSetter;

public class LocationSerialization {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();
    private final PolicySerialization policy = new PolicySerialization();

    public Location serialize(org.atlasapi.content.Location location) {
        if (location == null) {
            return null;
        }
        Location internal =
                new Location();

        identifiedSetter.serialize(internal, location);

        internal.setAvailable(location.getAvailable());
        internal.setTransportIsLive(location.getTransportIsLive());
        TransportSubType transportSubType = location.getTransportSubType();
        if (transportSubType != null) {
            internal.setTransportSubType(transportSubType.name());
        }
        TransportType transportType = location.getTransportType();
        if (transportType != null) {
            internal.setTransportType(transportType.name());
        }
        internal.setUri(location.getUri());
        internal.setEmbedCode(location.getEmbedCode());
        internal.setEmbedId(location.getEmbedId());
        internal.setPolicy(policy.serialize(location.getPolicy()));

        return internal;
    }

    public org.atlasapi.content.Location deserialize(Location internal) {
        org.atlasapi.content.Location location = new org.atlasapi.content.Location();

        identifiedSetter.deserialize(location, internal);

        if (internal.getAvailable() != null) {
            location.setAvailable(internal.getAvailable());
        }

        location.setTransportIsLive(internal.getTransportIsLive());

        if (internal.getTransportSubType() != null) {
            location.setTransportSubType(TransportSubType.valueOf(internal.getTransportSubType()));
        }
        if (internal.getTransportType() != null) {
            location.setTransportType(TransportType.valueOf(internal.getTransportType()));
        }

        location.setUri(internal.getUri());
        location.setEmbedCode(internal.getEmbedCode());
        location.setEmbedId(internal.getEmbedId());
        location.setPolicy(policy.deserialize(internal.getPolicy()));

        return location;
    }

}