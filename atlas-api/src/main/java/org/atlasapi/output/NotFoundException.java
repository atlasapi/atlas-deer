package org.atlasapi.output;

import org.atlasapi.entity.Id;
import org.atlasapi.query.common.QueryExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

public class NotFoundException extends QueryExecutionException {

    private final Id missingResource;

    public NotFoundException(Id misssingResouce) {
        this.missingResource = checkNotNull(misssingResouce);
    }

    public Id getMissingResource() {
        return missingResource;
    }

    @Override
    public String getMessage() {
        return missingResource.toString();
    }

}
