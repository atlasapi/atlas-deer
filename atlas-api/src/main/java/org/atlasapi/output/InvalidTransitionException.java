package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

public class InvalidTransitionException extends QueryExecutionException {

    private static final long serialVersionUID = -4886172001098182242L;

    public InvalidTransitionException(String message) {
        super(message);
    }
}
