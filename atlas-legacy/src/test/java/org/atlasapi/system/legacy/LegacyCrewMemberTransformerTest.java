package org.atlasapi.system.legacy;

import org.atlasapi.content.Actor;
import org.atlasapi.content.CrewMember;
import org.atlasapi.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LegacyCrewMemberTransformerTest {
    private LegacyCrewMemberTransformer transformer = new LegacyCrewMemberTransformer();

    @Test
    public void testActors() {

        org.atlasapi.media.entity.Actor legacyActor =
                new org.atlasapi.media.entity.Actor("barney", "bam bam", Publisher.BETTY);

        legacyActor.withCharacter("rubble")
                .withName("mel blanc")
                .withProfileLink("http://rip");

        Actor expected = new Actor("barney", "bam bam", Publisher.BETTY);
        expected.withCharacter("rubble")
                .withName("mel blanc")
                .withProfileLink("http://rip");

        Actor actual = transformer.translateLegacyActor(legacyActor);
        assertThat(actual, is(notNullValue()));
        assertActorsEqual(expected, actual);
    }

    @Test
    public void testWithCrewMembersMissingIds() {
        org.atlasapi.media.entity.CrewMember legacyCrewMember =
                org.atlasapi.media.entity.CrewMember.crewMemberWithoutId("barney", "unknown", Publisher.BETTY);

        CrewMember actual = transformer.apply(legacyCrewMember);
        assertThat(actual, is(notNullValue()));
    }

    @Test
    public void testGeneralCrewMember() {
        org.atlasapi.media.entity.CrewMember legacyCrewMember =
                new org.atlasapi.media.entity.CrewMember("barney", "bam bam", Publisher.BETTY);

        legacyCrewMember.setId(456L);

        CrewMember expected = new CrewMember("barney", "bam bam", Publisher.BETTY);
        expected.setId(456L);

        CrewMember actual = transformer.apply(legacyCrewMember);
        assertThat(actual, is(notNullValue()));
        assertThat(actual instanceof Actor, is(false));
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
        assertThat(actual, is(notNullValue()));
        assertThat(actual instanceof Actor, is(false));
        assertCrewMembersEqual(expected, actual);
    }

    private void assertIdentifiedEqual(Identified expected, Identified actual) {
        assertThat(actual.getId(), is(expected.getId()));
        // try out the equality operator (but useless for debugging)
        assertThat(actual, is(expected));
    }

    private void assertCrewMembersEqual(CrewMember expected, CrewMember actual) {
        // Identified doesn't regard aliases as important for equality

        assertThat(actual.profileLinks(), is(expected.profileLinks()));
        assertThat(actual.role(), is(expected.role()));
        assertIdentifiedEqual(expected, actual);
    }

    private void assertActorsEqual(Actor expected, Actor actual) {
        assertThat(actual.character(), is(expected.character()));
        assertCrewMembersEqual(expected, actual);
    }
}
