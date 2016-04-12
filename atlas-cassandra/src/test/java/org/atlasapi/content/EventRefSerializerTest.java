package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.entity.Publisher;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EventRefSerializerTest {

    private final EventRefSerializer eventRefSerializer = new EventRefSerializer();

    @Test
    public void eventRefSerializeTest() {
        EventRef eventRef = new EventRef(Id.valueOf(12345), Publisher.ADAPT_BBC_PODCASTS);
        EventRef deserialized = eventRefSerializer.deserialize(eventRefSerializer.serialize(eventRef));
        assertThat(eventRef.getId().toString(), is(deserialized.getId().toString()));
        assertThat(
                eventRef.getResourceType().getKey(),
                is(deserialized.getResourceType().getKey())
        );
    }
}
