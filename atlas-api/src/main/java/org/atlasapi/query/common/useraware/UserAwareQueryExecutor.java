package org.atlasapi.query.common.useraware;

import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;

public interface UserAwareQueryExecutor<T> {

    UserAwareQueryResult<T> execute(UserAwareQuery<T> query) throws QueryExecutionException;

}
