package org.atlasapi.entity;

import java.util.Objects;

import javax.annotation.Nullable;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class Rating implements Hashable {

    private final float value;
    private final String type;
    private final Publisher publisher;
    private final Long numberOfVotes;

    public Rating(String type, float value, Publisher publisher, @Nullable Long numberOfVotes) {
        this.type = checkNotNull(type);
        this.value = value;
        this.publisher = checkNotNull(publisher);
        this.numberOfVotes = numberOfVotes;
    }

    public float getValue() {
        return value;
    }

    public String getType() {
        return type;
    }

    public Publisher getPublisher() {
        return publisher;
    }

    public Long getNumberOfVotes() {
        return numberOfVotes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Rating rating = (Rating) o;
        return Float.compare(rating.value, value) == 0 &&
                Objects.equals(type, rating.type) &&
                publisher == rating.publisher &&
                Objects.equals(numberOfVotes, rating.numberOfVotes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type, publisher);
    }

    @Override
    public String toString() {
        return "Rating{" +
                "value=" + value +
                ", type='" + type + '\'' +
                ", publisher=" + publisher +
                ", numberOfVotes=" + numberOfVotes +
                '}';
    }
}
