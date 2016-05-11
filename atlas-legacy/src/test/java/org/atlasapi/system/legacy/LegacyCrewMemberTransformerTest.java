package org.atlasapi.system.legacy;

import org.atlasapi.content.Actor;
import org.atlasapi.content.CrewMember;
import org.atlasapi.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static org.junit.Assert.*;

public class LegacyCrewMemberTransformerTest {
    private LegacyCrewMemberTransformer transformer = new LegacyCrewMemberTransformer();

    @Test
    public void testActors() {

        org.atlasapi.media.entity.Actor legacyActor =
                new org.atlasapi.media.entity.Actor("barney", "bam bam", Publisher.BETTY);

        legacyActor.withCharacter("rubble")
                .withName("mel blanc")
                .withProfileLink("http://rip")
                .setId(666L);

        Actor expected = new Actor("barney", "bam bam", Publisher.BETTY);
        expected.withCharacter("rubble")
                .withName("mel blanc")
                .withProfileLink("http://rip")
                .setId(666L);

        Actor actual = transformer.translateLegacyActor(legacyActor);
        assertNotNull(actual);
        assertActorsEqual(expected, actual);
    }

    @Test
    public void testWithCrewMembersMissingIds() {
        org.atlasapi.media.entity.CrewMember legacyCrewMember =
                org.atlasapi.media.entity.CrewMember.crewMemberWithoutId("barney", "unknown", Publisher.BETTY);

        CrewMember actual = transformer.apply(legacyCrewMember);
        assertNull(actual);
    }

    @Test
    public void testGeneralCrewMember() {
        org.atlasapi.media.entity.CrewMember legacyCrewMember =
                new org.atlasapi.media.entity.CrewMember("barney", "bam bam", Publisher.BETTY);

        legacyCrewMember.setId(456L);

        CrewMember expected = new CrewMember("barney", "bam bam", Publisher.BETTY);
        expected.setId(456L);

        CrewMember actual = transformer.apply(legacyCrewMember);
        assertNotNull(actual);
        assertFalse(actual instanceof Actor);

        assertCrewMembersEqual(expected, actual);
    }

    @Test
    public void testUnknownCrewMemberType() {
        class UnknownCrewMember extends org.atlasapi.media.entity.CrewMember {
            public UnknownCrewMember(String uri, String curie, Publisher publisher) {
                super(uri, curie, publisher);
                this.withRole(Role.UNKNOWN);
            }
        }

        UnknownCrewMember legacyCrewMember = new UnknownCrewMember("barney", "bam bam", Publisher.BETTY);
        legacyCrewMember.setId(678L);

        CrewMember expected = new CrewMember("barney", "bam bam", Publisher.BETTY);
        expected.withRole(CrewMember.Role.UNKNOWN)
                .setId(678L);

        CrewMember actual = transformer.apply(legacyCrewMember);
        assertNotNull(actual);
        assertFalse(actual instanceof Actor);

        assertCrewMembersEqual(expected, actual);
    }

    private void assertIdentifiedEqual(Identified expected, Identified actual) {
        assertEquals(expected.getId(), actual.getId());
        // try out the equality operator (but useless for debugging)
        assertEquals(expected, actual);
    }

    private void assertCrewMembersEqual(CrewMember expected, CrewMember actual) {
        // Identified doesn't regard aliases as important for equality
        assertEquals(expected.profileLinks(), actual.profileLinks());
        assertEquals(expected.role(), actual.role());
        assertIdentifiedEqual(expected, actual);
    }

    private void assertActorsEqual(Actor expected, Actor actual) {
        assertEquals(expected.character(), actual.character());
        assertCrewMembersEqual(expected, actual);
    }
}
