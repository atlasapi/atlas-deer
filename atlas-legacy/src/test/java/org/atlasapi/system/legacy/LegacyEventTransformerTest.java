package org.atlasapi.system.legacy;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.ContentCategory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LegacyEventTransformerTest {

    private LegacyEventTransformer eventTransformer;

    @Before
    public void setUp() throws Exception {
        eventTransformer = new LegacyEventTransformer();
    }

    @Test
    public void testTransformation() throws Exception {
        Event input = getEvent();

        org.atlasapi.event.Event event = eventTransformer.apply(input);

        checkEvent(event, input);
    }

    private Event getEvent() {
        Topic venue = new Topic(0L);
        venue.setNamespace("nsA");
        venue.setValue("valueA");

        Topic eventGroup = new Topic(1L);
        eventGroup.setNamespace("nsB");
        eventGroup.setValue("valueB");
        Organisation organisation = new Organisation();
        organisation.setCanonicalUri("uri");

        Event input = Event.builder()
                .withTitle("title")
                .withPublisher(Publisher.BBC)
                .withVenue(venue)
                .withStartTime(DateTime.now())
                .withEndTime(DateTime.now())
                .withParticipants(ImmutableList.of(new Person()))
                .withOrganisations(ImmutableList.of(organisation))
                .withEventGroups(ImmutableList.of(eventGroup))
                .withContent(ImmutableList.of(new ChildRef(0L, "uri", "sort", DateTime.now(),
                        EntityType.ITEM
                )))
                .build();

        input.setId(0L);
        input.setCanonicalUri("canonical");
        input.setCurie("curie");
        input.setAliasUrls(ImmutableList.of("url"));
        input.setAliases(ImmutableList.of(new Alias("ns", "value")));
        input.setEquivalentTo(ImmutableSet.of(
                new LookupRef("uri", 0L, Publisher.BBC, ContentCategory.CHILD_ITEM))
        );
        input.setLastUpdated(DateTime.now());
        input.setEquivalenceUpdate(DateTime.now());

        return input;
    }

    private void checkEvent(org.atlasapi.event.Event event, Event input) {
        assertThat(event.getTitle(), is(input.title()));
        assertThat(event.getSource(), is(input.publisher()));
        assertThat(event.getVenue().getId().longValue(), is(0L));
        assertThat(event.getStartTime(), is(input.startTime()));
        assertThat(event.getEndTime(), is(input.endTime()));
        assertThat(event.getParticipants().size(), is(input.participants().size()));
        assertThat(event.getOrganisations().size(), is(input.organisations().size()));
        assertThat(event.getEventGroups().size(), is(input.eventGroups().size()));
        assertThat(event.getContent().size(), is(input.content().size()));
        assertThat(
                event.getContent().get(0).getId().longValue(),
                is(input.content().get(0).getId())
        );
        assertThat(event.getContent().get(0).getSource(), is(input.publisher()));

        assertThat(event.getId().longValue(), is(input.getId()));
        assertThat(event.getCanonicalUri(), is(input.getCanonicalUri()));
        assertThat(event.getCurie(), is(input.getCurie()));
        assertThat(event.getAliasUrls(), is(input.getAliasUrls()));
        assertThat(event.getAliases().size(), is(input.getAliases().size()));
        assertThat(
                event.getAliases().iterator().next(),
                is(new org.atlasapi.entity.Alias("ns", "value"))
        );
        assertThat(event.getEquivalentTo().size(), is(event.getEquivalentTo().size()));
        assertThat(
                event.getEquivalentTo().iterator().next(),
                is(new EquivalenceRef(Id.valueOf(0L), Publisher.BBC))
        );
        assertThat(event.getLastUpdated(), is(input.getLastUpdated()));
        assertThat(event.getEquivalenceUpdate(), is(input.getEquivalenceUpdate()));
    }
}