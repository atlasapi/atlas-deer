package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.serialization.setters.IdentifiedSetter;
import org.atlasapi.media.entity.Publisher;

public class CrewMemberSerialization {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();

    public CrewMember serialize(org.atlasapi.content.CrewMember crewMember) {
        if (crewMember == null) {
            return null;
        }
        CrewMember internal =
                new CrewMember();

        identifiedSetter.serialize(internal, crewMember);

        org.atlasapi.content.CrewMember.Role role = crewMember.role();
        if (role != null) {
            internal.setRole(role.key());
        }
        internal.setName(crewMember.name());
        Publisher publisher = crewMember.publisher();
        if (publisher != null) {
            internal.setPublisher(publisher.key());
        }

        return internal;
    }

    public org.atlasapi.content.CrewMember deserialize(CrewMember internal) {
        if (internal == null) {
            return null;
        }
        org.atlasapi.content.CrewMember crewMember = new org.atlasapi.content.CrewMember();

        identifiedSetter.deserialize(crewMember, internal);

        crewMember = crewMember
                .withRole(org.atlasapi.content.CrewMember.Role.fromKey(internal.getRole()))
                .withName(internal.getName())
                .withPublisher(Publisher.fromKey(internal.getPublisher()).requireValue());

        return crewMember;
    }

}