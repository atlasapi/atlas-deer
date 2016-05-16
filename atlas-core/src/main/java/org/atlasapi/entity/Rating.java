package org.atlasapi.entity;

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

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Rating rating = (Rating) o;

        if (Float.compare(rating.value, value) != 0)
            return false;
        if (!type.equals(rating.type))
            return false;
        return publisher == rating.publisher;

    }

    @Override public int hashCode() {
        int result = (value != +0.0f ? Float.floatToIntBits(value) : 0);
        result = 31 * result + type.hashCode();
        result = 31 * result + publisher.hashCode();
        return result;
    }
}
