package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.CrewMember;
import org.atlasapi.media.entity.Publisher;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class CrewMemberSerializationTest {

    private CrewMemberSerialization serialization;

    @Before
    public void setUp() throws Exception {
        serialization = new CrewMemberSerialization();
    }

    @Test
    public void deserializationHandlesNullRoles() throws Exception {
        @SuppressWarnings("ConstantConditions")
        CrewMember crewMember = new CrewMember("uri", "", Publisher.METABROADCAST)
                .withRole(null);

        assertThat(
                serialization.deserialize(
                        serialization.serialize(
                                crewMember
                        )
                )
                        .role(),
                is(nullValue())
        );
    }
}
