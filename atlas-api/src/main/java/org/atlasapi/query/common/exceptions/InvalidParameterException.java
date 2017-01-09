package org.atlasapi.query.common.exceptions;

import org.atlasapi.content.QueryParseException;

public class InvalidParameterException extends QueryParseException {

    public InvalidParameterException() {
        super();
    }

    public InvalidParameterException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidParameterException(String message) {
        super(message);
    }

    public InvalidParameterException(Throwable cause) {
        super(cause);
    }

}
