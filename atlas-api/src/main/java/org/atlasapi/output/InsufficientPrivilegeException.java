package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

public class InsufficientPrivilegeException extends QueryExecutionException {

    private static final long serialVersionUID = 1L;

    public InsufficientPrivilegeException(String message) {
        super(message);
    }

}
