package org.atlasapi.eventV2;

import org.atlasapi.entity.Id;
import org.atlasapi.event.Event;
import org.atlasapi.media.entity.Publisher;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class EventV2Test {

    @Test
    public void testBuilderPopulatesSuperclassFields() throws Exception {
        Event event = Event.builder()
                .withId(Id.valueOf(0L))
                .withTitle("title")
                .withSource(Publisher.BBC)
                .build();

        assertThat(event.getId(), is(Id.valueOf(0L)));
        assertThat(event.getTitle(), is("title"));
        assertThat(event.getSource(), is(Publisher.BBC));
    }

}