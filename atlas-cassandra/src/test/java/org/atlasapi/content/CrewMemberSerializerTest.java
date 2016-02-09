package org.atlasapi.content;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CrewMemberSerializerTest {

    private final CrewMemberSerializer serializer = new CrewMemberSerializer();

    @Test
    public void testDeSerializeCrewMember() {
        CrewMember member = CrewMember
                .crewMember("id", "Jim", "director", Publisher.BBC)
                .withProfileLinks(ImmutableSet.of("linkOne", "linkTwo"));

        CrewMember deserialized = serializer.deserialize(serializer.serialize(member));

        checkMemeberProperties(member, deserialized);
    }

    @Test
    public void testDeSerializeActor() {

        Actor actor = Actor.actor("id", "name", "character", Publisher.BBC);

        CrewMember deserialized = serializer.deserialize(serializer.serialize(actor));

        assertThat(deserialized, is(instanceOf(Actor.class)));
        checkMemeberProperties(actor, deserialized);
        assertThat(((Actor) deserialized).character(), is(actor.character()));

    }

    private void checkMemeberProperties(CrewMember member, CrewMember deserialized) {
        assertThat(deserialized.getCanonicalUri(), is(member.getCanonicalUri()));
        assertThat(deserialized.name(), is(member.name()));
        assertThat(deserialized.role(), is(member.role()));
        assertThat(deserialized.profileLinks(), is(member.profileLinks()));
    }

}
