package org.atlasapi.entity;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.Described;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Priority;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.Synopses;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DescribedSerializerTest {

    private DescribedSerializer<Described> serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new DescribedSerializer<>();
    }

    @Test
    public void testSerialization() throws Exception {
        Described expected = getDescribed();

        CommonProtos.Described serialized = serializer.serialize(expected);

        Described actual = serializer.deserialize(serialized, new Item());

        checkIdentified(actual, expected);
        checkDescribed(actual, expected);
    }

    @Test
    public void testSerializationIsNullSafe() throws Exception {
        Described expected = new Item();

        CommonProtos.Described serialized = serializer.serialize(expected);

        Described actual = serializer.deserialize(serialized, new Item());

        checkIdentified(actual, expected);
        checkDescribed(actual, expected);
    }

    private Described getDescribed() {
        Described described = new Item();

        setIdentifiedFields(described);
        setDescribedFields(described);

        return described;
    }

    private void setIdentifiedFields(Identified identified) {
        identified.setId(Id.valueOf(0L));
        identified.setCanonicalUri("url");
        identified.setCurie("curie");
        identified.setAliases(Sets.newHashSet(new Alias("ns", "val")));
        identified.setEquivalentTo(Sets.newHashSet(new EquivalenceRef(
                Id.valueOf(1L), Publisher.BBC)));
        identified.setLastUpdated(DateTime.now().withZone(DateTimeZone.UTC));
        identified.setEquivalenceUpdate(DateTime.now().withZone(DateTimeZone.UTC));
    }

    private void setDescribedFields(Described described) {
        described.setTitle("title");
        described.setShortDescription("short");
        described.setMediumDescription("medium");
        described.setLongDescription("long");
        described.setSynopses(new Synopses());
        described.setDescription("description");
        described.setMediaType(MediaType.VIDEO);
        described.setSpecialization(Specialization.FILM);
        described.setGenres(Lists.newArrayList("genre"));
        described.setPublisher(Publisher.AMAZON_UK);
        described.setImage("image");
        described.setImages(Lists.newArrayList(new Image("imageUri")));
        described.setThumbnail("thumbnail");
        described.setFirstSeen(DateTime.now().withZone(DateTimeZone.UTC));
        described.setLastFetched(DateTime.now().withZone(DateTimeZone.UTC));
        described.setThisOrChildLastUpdated(DateTime.now().withZone(DateTimeZone.UTC));
        described.setScheduleOnly(false);
        described.setActivelyPublished(true);
        described.setPresentationChannel("channel");
        described.setPriority(new Priority());
        described.setRelatedLinks(Lists.newArrayList(
                new RelatedLink.Builder(RelatedLink.LinkType.FACEBOOK, "type").build()
        ));
    }

    private void checkIdentified(Identified expected, Identified actual) {
        MatcherAssert.assertThat(actual.getId(), is(expected.getId()));
        MatcherAssert.assertThat(actual.getCanonicalUri(), is(expected.getCanonicalUri()));
        MatcherAssert.assertThat(actual.getCurie(), is(expected.getCurie()));
        MatcherAssert.assertThat(actual.getAliases(), is(expected.getAliases()));
        MatcherAssert.assertThat(actual.getEquivalentTo(), is(expected.getEquivalentTo()));
        MatcherAssert.assertThat(actual.getLastUpdated(), is(expected.getLastUpdated()));
        MatcherAssert.assertThat(actual.getEquivalenceUpdate(), is(expected.getEquivalenceUpdate()));
    }

    private void checkDescribed(Described actual, Described expected) {
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getShortDescription(), is(expected.getShortDescription()));
        assertThat(actual.getMediumDescription(), is(expected.getMediumDescription()));
        assertThat(actual.getLongDescription(), is(expected.getLongDescription()));

        if(expected.getSynopses() == null) {
            assertThat(actual.getSynopses(), nullValue());
        }
        else {
            assertThat(actual.getSynopses().getShortDescription(),
                    is(expected.getSynopses().getShortDescription()));
        }

        assertThat(actual.getDescription(), is(expected.getDescription()));
        assertThat(actual.getMediaType(), is(expected.getMediaType()));
        assertThat(actual.getSpecialization(), is(expected.getSpecialization()));
        assertThat(actual.getGenres(), is(expected.getGenres()));
        assertThat(actual.getSource(), is(expected.getSource()));
        assertThat(actual.getImage(), is(expected.getImage()));
        assertThat(actual.getImages(), is(expected.getImages()));
        assertThat(actual.getThumbnail(), is(expected.getThumbnail()));
        assertThat(actual.getFirstSeen(), is(expected.getFirstSeen()));
        assertThat(actual.getLastFetched(), is(expected.getLastFetched()));
        assertThat(actual.getThisOrChildLastUpdated(), is(expected.getThisOrChildLastUpdated()));
        assertThat(actual.isScheduleOnly(), is(expected.isScheduleOnly()));
        assertThat(actual.isActivelyPublished(), is(expected.isActivelyPublished()));
        assertThat(actual.getPresentationChannel(), is(expected.getPresentationChannel()));

        if(expected.getPriority() == null) {
            assertThat(actual.getPriority(), nullValue());
        }
        else {
            assertThat(actual.getPriority().getPriority(), is(expected.getPriority().getPriority()));
        }
        assertThat(actual.getRelatedLinks(), is(expected.getRelatedLinks()));
    }
}