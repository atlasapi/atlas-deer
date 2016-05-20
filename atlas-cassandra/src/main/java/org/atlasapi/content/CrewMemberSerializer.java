package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;

import com.google.common.collect.ImmutableSet;

public class CrewMemberSerializer {

    public ContentProtos.CrewMember serialize(CrewMember crew) {
        ContentProtos.CrewMember.Builder crewMember = ContentProtos.CrewMember.newBuilder();
        if (crew.getCanonicalUri() != null) {
            crewMember.setUri(crew.getCanonicalUri());
        }
        if (crew.role() != null) {
            crewMember.setRole(crew.role().key());
        }
        if (crew.name() != null) {
            crewMember.setName(crew.name());
        }
        if (crew instanceof Actor) {
            String character = ((Actor) crew).character();
            if (character != null) {
                crewMember.setCharacter(character);
            }
        }
        crewMember.addAllProfileLinks(crew.profileLinks());
        return crewMember.build();
    }

    public CrewMember deserialize(ContentProtos.CrewMember crewMember) {
        CrewMember member = new CrewMember();

        // roles are optional, but used as a determining factor for Actor vs CrewMember
        if (crewMember.hasRole()) {
            CrewMember.Role role = CrewMember.Role.fromKey(crewMember.getRole());

            if (role == CrewMember.Role.ACTOR) {
                member = new Actor();
                if (crewMember.hasCharacter()) {
                    ((Actor) member).withCharacter(crewMember.getCharacter());
                }

            } else {
                member.withRole(role);
            }
        }


        if (crewMember.hasName()) {
            member.withName(crewMember.getName());
        }

        if (crewMember.hasUri()) {
            member.setCanonicalUri(crewMember.getUri());
        }

        member.withProfileLinks(ImmutableSet.copyOf(crewMember.getProfileLinksList()));

        return member;
    }

}
