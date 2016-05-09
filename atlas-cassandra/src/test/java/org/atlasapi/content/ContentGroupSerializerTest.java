package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ContentGroupSerializerTest {

    private ContentGroupSerializer<ContentGroup> serializer;

    @Before
    public void setUp() throws Exception {
        serializer = new ContentGroupSerializer<>();
    }

    @Test
    public void testSerialization() throws Exception {
        ContentGroup expected = new ContentGroup();

        expected.setType(ContentGroup.Type.FRANCHISE);
        expected.addContent(new ItemRef(Id.valueOf(0L), Publisher.BBC, "sort",
                DateTime.now().withZone(DateTimeZone.UTC)
        ));
        expected.setTitle("title");

        CommonProtos.ContentGroup serialized = serializer.serialize(expected);
        ContentGroup actual = serializer.deserialize(serialized, new ContentGroup());

        checkContentGroup(expected, actual);
        checkDescribed(expected, actual);
    }

    @Test
    public void testSerializationWithRatingsAndReviews() {
        ContentGroup expected = new ContentGroup();
        addReviewsAndRatingsToDescribed(expected);
        CommonProtos.ContentGroup serialized = serializer.serialize(expected);
        ContentGroup actual = serializer.deserialize(serialized, new ContentGroup());
        checkDescribed(expected, actual);
    }

    private void checkContentGroup(ContentGroup expected, ContentGroup actual) {
        assertThat(actual.getType(), is(expected.getType()));
        assertThat(actual.getContents(), is(expected.getContents()));
    }

    private void checkDescribed(Described expected, Described actual) {
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat("Same number of reviews", actual.getReviews().size(), is(expected.getReviews().size()));
        assertTrue("All reviews present", actual.getReviews().containsAll(expected.getReviews()));
        assertThat("Same number of ratings", actual.getRatings().size(), is(expected.getRatings().size()));
        assertTrue("All ratings present", actual.getRatings().containsAll(expected.getRatings()));
    }

    private void addReviewsAndRatingsToDescribed(Described described) {
        described.setReviews(Arrays.asList(
                new Review(Locale.ENGLISH, "dog's bolls"),
                new Review(Locale.CHINESE, "hen hao"),
                new Review(Locale.FRENCH, "tres bien"),
                new Review(null, "sehr gut")
        ));

        described.setRatings(Arrays.asList(
                new Rating("5STAR", 3.0f, Publisher.RADIO_TIMES),
                new Rating("MOOSE", 1.0f, Publisher.BBC)
        ));
    }
}