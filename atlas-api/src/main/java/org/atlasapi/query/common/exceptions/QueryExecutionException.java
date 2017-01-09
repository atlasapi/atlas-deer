package org.atlasapi.query.common.exceptions;

public class QueryExecutionException extends Exception {

    private static final long serialVersionUID = 1L;

    public QueryExecutionException() {
        super();
    }

    public QueryExecutionException(String message) {
        super(message);
    }

    public QueryExecutionException(Throwable cause) {
        super(cause);
    }

    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

}
