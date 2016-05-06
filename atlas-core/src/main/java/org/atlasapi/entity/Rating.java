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
}
