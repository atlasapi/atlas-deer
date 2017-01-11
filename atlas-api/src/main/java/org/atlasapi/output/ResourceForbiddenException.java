package org.atlasapi.output;

import org.atlasapi.query.common.exceptions.QueryExecutionException;

public class ResourceForbiddenException extends QueryExecutionException {

    private static final long serialVersionUID = -1250887434562208920L;

    public ResourceForbiddenException() {
        super("You do not have permission to access this resource");
    }
}
