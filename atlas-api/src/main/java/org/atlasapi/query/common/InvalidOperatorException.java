package org.atlasapi.query.common;

import org.atlasapi.content.QueryParseException;

public class InvalidOperatorException extends QueryParseException {

    public InvalidOperatorException() {
        super();
    }

    public InvalidOperatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidOperatorException(String message) {
        super(message);
    }

    public InvalidOperatorException(Throwable cause) {
        super(cause);
    }

}
