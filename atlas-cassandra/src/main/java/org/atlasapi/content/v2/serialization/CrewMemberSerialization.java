package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.serialization.setters.IdentifiedSetter;
import org.atlasapi.media.entity.Publisher;

public class CrewMemberSerialization {

    private static final String TYPE_ACTOR = "Actor";
    private static final String TYPE_CREW_MEMBER = "CrewMember";
    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();

    public CrewMember serialize(org.atlasapi.content.CrewMember crewMember) {
        if (crewMember == null) {
            return null;
        }

        CrewMember internal = new CrewMember();

        identifiedSetter.serialize(internal, crewMember);

        if (crewMember instanceof org.atlasapi.content.Actor) {
            internal.setType(TYPE_ACTOR);
            internal.setCharacter(((org.atlasapi.content.Actor) crewMember).character());
        } else {
            internal.setType(TYPE_CREW_MEMBER);
        }

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

        org.atlasapi.content.CrewMember crewMember;
        String type = internal.getType();

        switch (type) {
        case TYPE_ACTOR:
            crewMember = new org.atlasapi.content.Actor();
            break;
        case TYPE_CREW_MEMBER:
            crewMember = new org.atlasapi.content.CrewMember();
            break;
        default:
            throw new IllegalArgumentException(String.format(
                    "type %s not recognised",
                    type
            ));
        }

        identifiedSetter.deserialize(crewMember, internal);

        String publisherKey = internal.getPublisher();
        Publisher publisher = Publisher.fromKey(publisherKey).valueOrNull();

        // The serialize method does not validate this is non-null and we have some crew members
        // written in the DB with null roles so we need to gracefully handle this case
        if (internal.getRole() != null) {
            crewMember.withRole(org.atlasapi.content.CrewMember.Role.fromKey(
                    internal.getRole())
            );
        }

        crewMember = crewMember
                .withName(internal.getName())
                .withPublisher(publisher);

        if (TYPE_ACTOR.equals(type)) {
            crewMember = ((org.atlasapi.content.Actor) crewMember).withCharacter(
                    internal.getCharacter()
            );
        }

        return crewMember;
    }

}
