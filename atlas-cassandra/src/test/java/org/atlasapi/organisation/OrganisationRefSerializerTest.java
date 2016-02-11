package org.atlasapi.organisation;
import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.CommonProtos;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class OrganisationRefSerializerTest {

    private OrganisationRefSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new OrganisationRefSerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        OrganisationRef expected = new OrganisationRef(Id.valueOf(12l),"uri");
        CommonProtos.Reference serialized = serializer.serialize(expected);
        OrganisationRef actual = serializer.deserialize(serialized);

        assertThat(expected, is(actual));
    }

}