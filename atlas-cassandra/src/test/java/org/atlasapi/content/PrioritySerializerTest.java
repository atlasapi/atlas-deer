package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.serialization.protobuf.CommonProtos;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class PrioritySerializerTest {

    private PrioritySerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new PrioritySerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        Priority expected = new Priority(0.0, Lists.newArrayList("reason"));

        CommonProtos.Priority serialized = serializer.serialize(expected);

        Priority actual = serializer.deserialize(serialized);

        assertThat(actual.getPriority(), is(expected.getPriority()));
        assertThat(actual.getReasons(), is(expected.getReasons()));
    }

    @Test
    public void testSerializationIsNullSafe() throws Exception {
        Priority expected = new Priority();

        CommonProtos.Priority serialized = serializer.serialize(expected);

        Priority actual = serializer.deserialize(serialized);

        expected.setReasons(Lists.newArrayList());
        assertThat(actual.getPriority(), is(expected.getPriority()));
        assertThat(actual.getReasons(), is(expected.getReasons()));
    }
}