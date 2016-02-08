package org.atlasapi.schedule;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.NoOpContentResolver;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.DateTimeZones;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ItemAndBroadcastSerializerTest {

    private final ItemAndBroadcastSerializer serializer = new ItemAndBroadcastSerializer(new ContentSerializer(
            new ContentSerializationVisitor(new NoOpContentResolver())));

    @Test
    public void testDeSerialization() {

        Item item = new Episode("episode", "episode", Publisher.BBC);
        item.setId(1);
        Broadcast broadcast = new Broadcast(
                Id.valueOf(1),
                DateTime.now(DateTimeZones.UTC),
                DateTime.now(DateTimeZones.UTC)
        );
        ItemAndBroadcast iab = new ItemAndBroadcast(item, broadcast);

        byte[] serialized = serializer.serialize(iab);
        ItemAndBroadcast deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getItem(), is(item));
        assertThat(deserialized.getBroadcast(), is(broadcast));

    }

}
