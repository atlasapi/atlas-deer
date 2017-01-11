package org.atlasapi.query.common.useraware;

import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;

public interface UserAccountsAwareQueryExecutor<T> {
    UserAccountsAwareQueryResult<T> execute(UserAccountsAwareQuery<T> query) throws QueryExecutionException;
}
