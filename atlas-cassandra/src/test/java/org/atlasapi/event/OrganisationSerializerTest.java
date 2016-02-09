package org.atlasapi.event;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class OrganisationSerializerTest {

    private OrganisationSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new OrganisationSerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        Organisation expected = new Organisation();

        expected.setMembers(Lists.newArrayList(new Person("uri", "curie", Publisher.BBC)));
        expected.setType(ContentGroup.Type.PERSON);

        CommonProtos.Organisation serialized = serializer.serialize(expected);
        Organisation actual = serializer.deserialize(serialized);

        checkOrganisation(expected, actual);
        checkContentGroup(expected, actual);
    }

    private void checkContentGroup(ContentGroup expected, ContentGroup actual) {
        assertThat(actual.getType(), is(expected.getType()));
    }

    private void checkOrganisation(Organisation expected, Organisation actual) {
        assertThat(actual.members().size(), is(expected.members().size()));
        assertThat(
                actual.members().get(0).getCanonicalUri(),
                is(expected.members().get(0).getCanonicalUri())
        );
    }
}