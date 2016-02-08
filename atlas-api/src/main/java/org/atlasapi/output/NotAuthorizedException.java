package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

public class NotAuthorizedException extends QueryExecutionException {

    private static final long serialVersionUID = 4567624251087993406L;

    @Override
    public String getMessage() {
        return "Access denied";
    }

}
