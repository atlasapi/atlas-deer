package org.atlasapi.event;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

public class EventTest {

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