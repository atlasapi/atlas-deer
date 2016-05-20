package org.atlasapi.entity;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import org.atlasapi.media.entity.Publisher;

public class Rating {

    private final float value;
    private final String type;
    private final Publisher publisher;

    public Rating(String type, float value, Publisher publisher) {
        this.type = checkNotNull(type);
        this.value = value;
        this.publisher = checkNotNull(publisher);
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
                publisher == rating.publisher;
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
                '}';
    }
}