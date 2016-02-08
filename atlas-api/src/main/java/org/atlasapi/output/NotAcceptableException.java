package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

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
