package org.atlasapi.output;

import org.atlasapi.query.common.exceptions.QueryExecutionException;

public class NotAcceptableException extends QueryExecutionException {

    public NotAcceptableException() {
        super();
    }

    public NotAcceptableException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotAcceptableException(String message) {
        super(message);
    }

    public NotAcceptableException(Throwable cause) {
        super(cause);
    }

}
