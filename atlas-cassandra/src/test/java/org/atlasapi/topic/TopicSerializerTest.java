package org.atlasapi.topic;

import java.io.File;
import java.nio.file.Files;

import org.atlasapi.content.Described;
import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TopicSerializerTest {

    private final TopicSerializer serializer = new TopicSerializer();

    @Test
    public void testDeSerializesTopic() {
        Topic expected = new Topic(1234);
        expected.setPublisher(Publisher.DBPEDIA);
        expected.setType(Topic.Type.PERSON);
        checkTopic(serializer.deserialize(serializer.serialize(expected)), expected);
        expected.addAlias(new Alias("a", "alias1"));
        checkTopic(serializer.deserialize(serializer.serialize(expected)), expected);
        expected.setTitle("Jim");
        checkTopic(serializer.deserialize(serializer.serialize(expected)), expected);
        expected.setDescription("Top Bloke");
        checkTopic(serializer.deserialize(serializer.serialize(expected)), expected);
        expected.setImage("suave");
        checkTopic(serializer.deserialize(serializer.serialize(expected)), expected);
        expected.setThumbnail("present");

        setIdentifiedProperties(expected);
        setDescribedProperties(expected);

        Topic actual = serializer.deserialize(serializer.serialize(expected));

        checkIdentifiedProperties(actual, expected);
        checkDescribedProperties(actual, expected);
        checkTopic(actual, expected);
    }

    @Test
    public void testDeserializationBackwardsCompatibility() throws Exception {
        Topic expected = new Topic(1234);
        expected.setPublisher(Publisher.BBC);
        expected.setType(Topic.Type.PERSON);
        expected.addAliases(Lists.newArrayList(new Alias("a", "alias1"), new Alias("b", "alias2")));
        expected.setTitle("title");
        expected.setDescription("desc");
        expected.setImage("image");
        expected.setThumbnail("thumbnail");

        File file = new File("src/test/resources/protoc/legacy-serialized-topic.bin");

        byte[] msg = Files.readAllBytes(file.toPath());

        Topic actual = serializer.deserialize(msg);

        checkTopic(actual, expected);
    }

    private void checkTopic(Topic actual, Topic expected) {
        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getSource(), is(expected.getSource()));
        assertThat(actual.getAliases(), is(expected.getAliases()));
        assertThat(actual.getType(), is(expected.getType()));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getDescription(), is(expected.getDescription()));
        assertThat(actual.getImage(), is(expected.getImage()));
        assertThat(actual.getThumbnail(), is(expected.getThumbnail()));
    }

    private void checkDescribedProperties(Described actual, Described expected) {
        checkIdentifiedProperties(actual, expected);
        assertThat(actual.getSource(), is(expected.getSource()));
        assertThat(actual.getDescription(), is(expected.getDescription()));
        assertThat(actual.getFirstSeen(), is(expected.getFirstSeen()));
        assertThat(actual.getGenres(), is(expected.getGenres()));
        assertThat(actual.getImage(), is(expected.getImage()));
        assertThat(actual.getImages(), is(expected.getImages()));
        assertThat(actual.getLongDescription(), is(expected.getLongDescription()));
        assertThat(actual.getMediaType(), is(expected.getMediaType()));
        assertThat(actual.getMediumDescription(), is(expected.getMediumDescription()));
        assertThat(actual.getPresentationChannel(), is(expected.getPresentationChannel()));
        assertThat(actual.isScheduleOnly(), is(expected.isScheduleOnly()));
        assertThat(actual.getShortDescription(), is(expected.getShortDescription()));
        assertThat(actual.getSpecialization(), is(expected.getSpecialization()));
        assertThat(actual.getThisOrChildLastUpdated(), is(expected.getThisOrChildLastUpdated()));
        assertThat(actual.getThumbnail(), is(expected.getThumbnail()));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.isActivelyPublished(), is(expected.isActivelyPublished()));
    }

    private void checkIdentifiedProperties(Identified actual, Identified expected) {
        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getAliases(), is(expected.getAliases()));
        assertThat(actual.getCanonicalUri(), is(expected.getCanonicalUri()));
        assertThat(actual.getEquivalentTo(), is(expected.getEquivalentTo()));
        assertThat(actual.getLastUpdated(), is(expected.getLastUpdated()));
    }

    private void setDescribedProperties(Described described) {
        setIdentifiedProperties(described);
        described.setPublisher(Publisher.BBC);
        described.setDescription("desc");
        described.setFirstSeen(DateTime.parse("2015-09-09T10:08:18.542Z"));
        described.setGenres(ImmutableSet.of("genre"));
        described.setImage("image");
        described.setImages(ImmutableSet.of(new Image("image")));
        described.setLongDescription("longDesc");
        described.setMediaType(MediaType.AUDIO);
        described.setMediumDescription("medDesc");
        described.setPresentationChannel("bbcone");
        described.setScheduleOnly(true);
        described.setShortDescription("shortDesc");
        described.setSpecialization(Specialization.RADIO);
        described.setThisOrChildLastUpdated(DateTime.parse("2015-09-09T10:08:18.543Z"));
        described.setThumbnail("thumbnail");
        described.setTitle("title");
    }

    private void setIdentifiedProperties(Identified identified) {
        identified.setId(Id.valueOf(1234));
        identified.setLastUpdated(DateTime.parse("2015-09-09T10:08:18.432Z"));
        identified.setAliases(ImmutableSet.of(new Alias("a", "alias1"), new Alias("b", "alias2")));
        identified.setCanonicalUri("canonicalUri");
        identified.setEquivalenceUpdate(DateTime.parse("2015-09-09T10:08:18.432Z"));
        identified.setEquivalentTo(ImmutableSet.of(new EquivalenceRef(
                Id.valueOf(1),
                Publisher.BBC
        )));
    }
}
