package org.atlasapi.system.legacy;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class LegacyPersonTransformerTest {

    private LegacyPersonTransformer transformer;

    @Before
    public void setUp() throws Exception {
        transformer = new LegacyPersonTransformer();
    }

    @Test
    public void testTransformation() throws Exception {
        Person input = getPerson();

        org.atlasapi.entity.Person person = transformer.apply(input);

        checkPerson(person, input);
    }

    private Person getPerson() {
        Person person = new Person();

        person.setTitle("title");
        person.setPublisher(Publisher.BBC);

        person.setType(ContentGroup.Type.PERSON);
        person.setContents(ImmutableList.of(
                new ChildRef(1111L, "uri", "sort", DateTime.now(), EntityType.ITEM)
        ));

        person.setGivenName("given");
        person.setFamilyName("family");
        person.setGender("M");
        person.setBirthDate(DateTime.now());
        person.setBirthPlace("place");
        person.setQuotes(ImmutableList.of("quote"));

        return person;
    }

    private void checkPerson(org.atlasapi.entity.Person person, Person input) {
        assertThat(person.getTitle(), is(input.getTitle()));
        assertThat(person.getType(), is(org.atlasapi.content.ContentGroup.Type.PERSON));
        checkContent(person, input);
        assertThat(person.getGivenName(), is(input.getGivenName()));
        assertThat(person.getFamilyName(), is(input.getFamilyName()));
        assertThat(person.getGender(), is(input.getGender()));
        assertThat(person.getBirthDate(), is(input.getBirthDate()));
        assertThat(person.getBirthPlace(), is(input.getBirthPlace()));
        assertThat(person.getQuotes().size(), is(input.getQuotes().size()));
        assertThat(person.getQuotes(), is(input.getQuotes()));
    }

    private void checkContent(org.atlasapi.entity.Person person, Person input) {
        ContentRef content = person.getContents().get(0);
        ChildRef inputContent = input.getContents().get(0);

        assertThat(content.getId().longValue(), is(inputContent.getId()));
        assertThat(content.getSource(), is(person.getSource()));
        assertThat(content.getContentType(), is(ContentType.ITEM));
    }
}