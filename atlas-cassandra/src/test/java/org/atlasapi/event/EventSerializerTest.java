package org.atlasapi.event;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Person;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class EventSerializerTest {

    private EventSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new EventSerializer();
    }

    @Test
    public void testSerialization() throws Exception {
        Event.Builder<?, ?> builder = Event.builder();

        setIdentifiedFields(builder);
        setEventFields(builder);

        Event expected = builder.build();

        byte[] msg = serializer.serialize(expected);
        Event actual = serializer.deserialize(msg);

        checkIdentified(expected, actual);
        checkEvent(expected, actual);
    }

    private void setIdentifiedFields(Event.Builder<?, ?> builder) {
        builder.withId(Id.valueOf(0L))
                .withCanonicalUri("url")
                .withCurie("curie")
                .withAliases(Sets.newHashSet(new Alias("ns", "val")))
                .withEquivalentTo(Sets.newHashSet(new EquivalenceRef(
                        Id.valueOf(1L), Publisher.BBC)))
                .withLastUpdated(DateTime.now().withZone(DateTimeZone.UTC))
                .withEquivalenceUpdate(DateTime.now().withZone(DateTimeZone.UTC));
    }

    private void setEventFields(Event.Builder<?, ?> builder) {
        builder.withTitle("title")
                .withSource(Publisher.BBC)
                .withVenue(new Topic(Id.valueOf(12L)))
                .withStartTime(DateTime.now().plusHours(1).withZone(DateTimeZone.UTC))
                .withEndTime(DateTime.now().plusHours(2).withZone(DateTimeZone.UTC))
                .withParticipants(Lists.newArrayList(new Person("a", "aa", Publisher.BBC)))
                .withOrganisations(Lists.newArrayList(new Organisation()))
                .withEventGroups(Lists.newArrayList(new Topic(Id.valueOf(2L))))
                .withContent(Lists.newArrayList(new ItemRef(Id.valueOf(21L),
                        Publisher.BBC, "sort", DateTime.now().withZone(DateTimeZone.UTC))));
    }

    private void checkIdentified(Identified expected, Identified actual) {
        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getCanonicalUri(), is(expected.getCanonicalUri()));
        assertThat(actual.getCurie(), is(expected.getCurie()));
        assertThat(actual.getAliases(), is(expected.getAliases()));
        assertThat(actual.getEquivalentTo(), is(expected.getEquivalentTo()));
        assertThat(actual.getLastUpdated(), is(expected.getLastUpdated()));
        assertThat(actual.getEquivalenceUpdate(), is(expected.getEquivalenceUpdate()));
    }

    private void checkEvent(Event expected, Event actual) {
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getSource(), is(expected.getSource()));
        assertThat(actual.getVenue(), is(expected.getVenue()));
        assertThat(actual.getStartTime(), is(expected.getStartTime()));
        assertThat(actual.getEndTime(), is(expected.getEndTime()));
        assertThat(actual.getParticipants().size(), is(expected.getParticipants().size()));
        assertThat(actual.getParticipants().get(0).getCanonicalUri(),
                is(expected.getParticipants().get(0).getCanonicalUri()));
        assertThat(actual.getOrganisations().size(), is(expected.getOrganisations().size()));
        assertThat(actual.getEventGroups(), is(expected.getEventGroups()));
        assertThat(actual.getContent(), is(expected.getContent()));
    }
}