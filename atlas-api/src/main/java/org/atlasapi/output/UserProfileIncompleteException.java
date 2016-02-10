package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

public class UserProfileIncompleteException extends QueryExecutionException {

    private static final long serialVersionUID = -1250887434562208920L;

    public UserProfileIncompleteException() {
        super("A completed user profile is required to access this resource");
    }
}
