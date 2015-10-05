package org.atlasapi.event;

public interface EventHasher {

    String hash(Event event);
}
