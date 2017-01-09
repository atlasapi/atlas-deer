package org.atlasapi.query.common.exceptions;

import static com.google.common.base.Preconditions.checkNotNull;

public class MissingResolvedDataException extends RuntimeException {

    private final String message;

    public MissingResolvedDataException(String message) {
        this.message = checkNotNull(message);
    }

    @Override
    public String getMessage() {
        return message;
    }

}
