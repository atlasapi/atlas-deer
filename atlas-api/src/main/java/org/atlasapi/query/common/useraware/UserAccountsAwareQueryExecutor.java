package org.atlasapi.query.common.useraware;

import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.QueryExecutionException;

public interface UserAccountsAwareQueryExecutor<T> {
    UserAccountsAwareQueryResult<T> execute(UserAccountsAwareQuery<T> query) throws QueryExecutionException;
}
