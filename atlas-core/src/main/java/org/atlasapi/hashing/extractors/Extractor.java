package org.atlasapi.hashing.extractors;

import java.util.Optional;

public abstract class Extractor {

    public Optional<String> extractValue(Object object) {
        if (isSupported(object)) {
            return Optional.of(extractValueInternal(object));
        }
        return Optional.empty();
    }

    protected abstract boolean isSupported(Object object);

    protected abstract String extractValueInternal(Object object);
}
