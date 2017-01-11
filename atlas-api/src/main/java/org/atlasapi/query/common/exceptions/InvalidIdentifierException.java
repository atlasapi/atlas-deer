package org.atlasapi.query.common.exceptions;

import org.atlasapi.content.QueryParseException;

public class InvalidIdentifierException extends QueryParseException {

    public InvalidIdentifierException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidIdentifierException(String message) {
        super(message);
    }
}
