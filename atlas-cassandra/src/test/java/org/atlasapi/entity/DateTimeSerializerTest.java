package org.atlasapi.entity;

import org.atlasapi.serialization.protobuf.CommonProtos;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DateTimeSerializerTest {

    private DateTimeSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new DateTimeSerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        DateTime expected = DateTime.parse("2015-01-01T12:00:00.500+0100");

        CommonProtos.DateTime serialized = serializer.serialize(expected);

        DateTime actual = serializer.deserialize(serialized);

        assertThat(actual, is(expected.withZone(DateTimeZone.UTC)));
    }
}