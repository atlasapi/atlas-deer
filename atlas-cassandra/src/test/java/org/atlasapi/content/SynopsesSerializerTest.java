package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.junit.Before;
import org.junit.Test;

public class SynopsesSerializerTest {

    private SynopsesSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new SynopsesSerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        Synopses expected = getSynopses();

        CommonProtos.Synopses serialized = serializer.serialize(expected);

        Synopses actual = serializer.deserialize(serialized);

        check(actual, expected);
    }

    @Test
    public void testSerializationIsNullSafe() throws Exception {
        Synopses expected = new Synopses();

        CommonProtos.Synopses serialized = serializer.serialize(expected);

        Synopses actual = serializer.deserialize(serialized);

        check(actual, expected);
    }

    public Synopses getSynopses() {
        Synopses synopses = new Synopses();
        synopses.setShortDescription("short");
        synopses.setMediumDescription("medium");
        synopses.setLongDescription("long");
        return synopses;
    }

    public void check(Synopses actual, Synopses expected) {
        assertThat(actual.getShortDescription(), is(expected.getShortDescription()));
        assertThat(actual.getMediumDescription(), is(expected.getMediumDescription()));
        assertThat(actual.getLongDescription(), is(expected.getLongDescription()));
    }
}