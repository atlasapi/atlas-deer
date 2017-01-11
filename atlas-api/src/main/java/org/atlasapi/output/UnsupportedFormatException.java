package org.atlasapi.output;

import org.atlasapi.query.common.exceptions.QueryExecutionException;

public class UnsupportedFormatException extends QueryExecutionException {

    public UnsupportedFormatException() {
        super();
    }

    public UnsupportedFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedFormatException(String message) {
        super(message);
    }

    public UnsupportedFormatException(Throwable cause) {
        super(cause);
    }

}
