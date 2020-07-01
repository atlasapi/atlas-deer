package org.atlasapi.messaging;

import org.atlasapi.content.Brand;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;

import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializationException;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class JacksonMessageSerializerTest {

    @Test
    public void testDeSerializationOfItemUpdateMessage() throws Exception {
        Item item = new Item(Id.valueOf(1), Publisher.BBC);
        item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        ResourceUpdatedMessage msg = new ResourceUpdatedMessage(
                "1",
                Timestamp.of(1234),
                item.toRef()
        );
        JacksonMessageSerializer<ResourceUpdatedMessage> serializer
                = JacksonMessageSerializer.forType(ResourceUpdatedMessage.class);

        byte[] serialized = serializer.serialize(msg);

        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));

    }

    @Test
    public void testDeSerializationOfEpisodeUpdateMessage() throws Exception {
        Episode episode = new Episode(Id.valueOf(1), Publisher.BBC);
        episode.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        ResourceUpdatedMessage msg = new ResourceUpdatedMessage(
                "1",
                Timestamp.of(1234),
                episode.toRef()
        );
        JacksonMessageSerializer<ResourceUpdatedMessage> serializer
                = JacksonMessageSerializer.forType(ResourceUpdatedMessage.class);

        byte[] serialized = serializer.serialize(msg);

        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));

    }

    @Test
    public void testDeSerializationOfBrandUpdateMessage() throws Exception {
        Brand brand = new Brand(Id.valueOf(1), Publisher.BBC);
        brand.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        ResourceUpdatedMessage msg = new ResourceUpdatedMessage(
                "1",
                Timestamp.of(1234),
                brand.toRef()
        );
        JacksonMessageSerializer<ResourceUpdatedMessage> serializer
                = JacksonMessageSerializer.forType(ResourceUpdatedMessage.class);

        byte[] serialized = serializer.serialize(msg);

        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));

    }

    @Test
    public void testDeSerializationOfTopicUpdateMessage() throws Exception {
        Topic topic = new Topic(Id.valueOf(1));
        topic.setPublisher(Publisher.BBC);
        topic.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        ResourceUpdatedMessage msg = new ResourceUpdatedMessage(
                "1",
                Timestamp.of(1234),
                topic.toRef()
        );
        JacksonMessageSerializer<ResourceUpdatedMessage> serializer
                = JacksonMessageSerializer.forType(ResourceUpdatedMessage.class);

        byte[] serialized = serializer.serialize(msg);

        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));

    }

    @Test
    public void testDeSerializationOfEquivalenceAssertionMessage() throws Exception {
        Topic topic = new Topic(Id.valueOf(1));
        topic.setPublisher(Publisher.BBC);
        topic.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        EquivalenceAssertionMessage msg = new EquivalenceAssertionMessage("1", Timestamp.of(1234),
                topic.toRef(), ImmutableSet.<ResourceRef>of(topic.toRef()), Publisher.all()
        );
        JacksonMessageSerializer<EquivalenceAssertionMessage> serializer
                = JacksonMessageSerializer.forType(EquivalenceAssertionMessage.class);

        byte[] serialized = serializer.serialize(msg);

        EquivalenceAssertionMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getSubject(), is(msg.getSubject()));
        assertThat(deserialized.getAssertedAdjacents(), is(msg.getAssertedAdjacents()));
        assertThat(deserialized.getPublishers(), is(msg.getPublishers()));

    }

    @Test
    public void testDeserializationOfEquivalentContentUpdatedMessage()
            throws MessageSerializationException, MessageDeserializationException {
        ContentRef contentRef = new ItemRef(
                Id.valueOf(0L),
                Publisher.BBC,
                "sortKey",
                DateTime.now()
        );
        EquivalentContentUpdatedMessage msg = new EquivalentContentUpdatedMessage(
                "1",
                Timestamp.of(42),
                2L,
                contentRef
        );

        JacksonMessageSerializer<EquivalentContentUpdatedMessage> serializer
                = JacksonMessageSerializer.forType(EquivalentContentUpdatedMessage.class);

        byte[] serialized = serializer.serialize(msg);

        EquivalentContentUpdatedMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getEquivalentSetId(), is(msg.getEquivalentSetId()));
        assertThat(deserialized.getContentRef(), is(contentRef));
    }

}
