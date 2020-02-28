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
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
        assertThat("All reviews present", actual.getReviews().containsAll(expected.getReviews()), is(true));
        assertThat("Same number of ratings", actual.getRatings().size(), is(expected.getRatings().size()));
        assertThat("All ratings present", actual.getRatings().containsAll(expected.getRatings()), is(true));
    }

    private void addReviewsAndRatingsToDescribed(Described described) {
        described.setReviews(Arrays.asList(
                Review.builder("dog's bolls").withLocale(Locale.ENGLISH).withSource(Optional.empty()).build(),
                Review.builder("hen hao").withLocale(Locale.CHINESE).withSource(Optional.empty()).build(),
                Review.builder("tres bien").withLocale(Locale.FRENCH).withSource(Optional.empty()).build(),
                Review.builder("sehr gut").withSource(Optional.empty()).build()
        ));

        described.setRatings(Arrays.asList(
                new Rating("5STAR", 3.0f, Publisher.RADIO_TIMES, 1234L),
                new Rating("MOOSE", 1.0f, Publisher.BBC, 1234L)
        ));
    }
}