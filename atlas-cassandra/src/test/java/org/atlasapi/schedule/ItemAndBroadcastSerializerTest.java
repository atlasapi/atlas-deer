package org.atlasapi.schedule;

import static org.junit.Assert.assertThat;
import org.testng.annotations.Test;
import static org.hamcrest.Matchers.is;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import com.metabroadcast.common.time.DateTimeZones;


public class ItemAndBroadcastSerializerTest {

    private final ItemAndBroadcastSerializer serializer = new ItemAndBroadcastSerializer();
    
    @Test
    public void testDeSerialization() {
        
        Item item = new Episode("episode", "episode", Publisher.BBC);
        item.setId(1);
        Broadcast broadcast = new Broadcast("channel", DateTime.now(DateTimeZones.UTC), DateTime.now(DateTimeZones.UTC));
        ItemAndBroadcast iab = new ItemAndBroadcast(item, broadcast);
        
        byte[] serialized = serializer.serialize(iab);
        ItemAndBroadcast deserialized = serializer.deserialize(serialized);
        
        assertThat(deserialized.getItem(), is(item));
        assertThat(deserialized.getBroadcast(), is(broadcast));
        
    }

}
