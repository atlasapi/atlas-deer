package org.atlasapi.output;

import org.atlasapi.query.common.QueryExecutionException;

public class NotAuthenticatedException extends QueryExecutionException {

    @Override
    public String getMessage() {
        return "Credentials are required";
    }

}
