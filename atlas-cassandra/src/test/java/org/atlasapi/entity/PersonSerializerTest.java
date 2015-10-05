package org.atlasapi.entity;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

public class PersonSerializerTest {

    private PersonSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new PersonSerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        Person expected = new Person();

        expected.setType(ContentGroup.Type.PERSON);
        expected.setGivenName("given");
        expected.setFamilyName("family");
        expected.setGender("M");
        expected.setBirthDate(DateTime.now().withZone(DateTimeZone.UTC));
        expected.setBirthPlace("place");
        expected.addQuote("quote");

        CommonProtos.Person serialized = serializer.serialize(expected);
        Person actual = serializer.deserialize(serialized);

        checkContentGroup(actual, expected);
        checkPerson(actual, expected);
    }

    private void checkContentGroup(ContentGroup actual, ContentGroup expected) {
        assertThat(actual.getType(), is(expected.getType()));
    }

    private void checkPerson(Person actual, Person expected) {
        assertThat(actual.getGivenName(), is(expected.getGivenName()));
        assertThat(actual.getFamilyName(), is(expected.getFamilyName()));
        assertThat(actual.getGender(), is(expected.getGender()));
        assertThat(actual.getBirthDate(), is(expected.getBirthDate()));
        assertThat(actual.getBirthPlace(), is(expected.getBirthPlace()));
        assertThat(actual.getQuotes(), is(expected.getQuotes()));
    }
}