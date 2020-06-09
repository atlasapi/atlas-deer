package org.atlasapi.entity;

import java.util.Optional;

import org.atlasapi.media.entity.Publisher;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RatingSerializerTest {

    private final RatingSerializer serializer = new RatingSerializer();

    @Test
    public void ratingSerializerTest() {
        Rating rating = new Rating("MOOSE", 1.0f, Publisher.BBC, 1234L);

        Optional<Rating> optionalDeserialized = serializer.deserialize(serializer.serialize(rating));
        Rating deserialized = optionalDeserialized.get();

        assertThat(rating.getPublisher(), is(deserialized.getPublisher()));
        assertThat(rating.getType(), is(deserialized.getType()));
        assertThat(rating.getValue(), is(deserialized.getValue()));
        assertThat(rating.getNumberOfVotes(), is(deserialized.getNumberOfVotes()));

        Rating ratingWithoutVotes = new Rating("MOOSE", 1.0f, Publisher.BBC);

        Optional<Rating> otherOptional = serializer.deserialize(serializer.serialize(rating));
        Rating deserializedWithoutVotes = optionalDeserialized.get();

        assertThat(rating.getPublisher(), is(deserializedWithoutVotes.getPublisher()));
        assertThat(rating.getType(), is(deserializedWithoutVotes.getType()));
        assertThat(rating.getValue(), is(deserializedWithoutVotes.getValue()));
        assertThat(rating.getNumberOfVotes(), is(deserializedWithoutVotes.getNumberOfVotes()));
    }

}