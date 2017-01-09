package org.atlasapi.application.users;

import java.util.List;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class UsersQueryExecutor implements UserAwareQueryExecutor<User> {

    private final UserStore userStore;

    public UsersQueryExecutor(UserStore userStore) {
        this.userStore = userStore;
    }

    @Override
    public UserAwareQueryResult<User> execute(UserAwareQuery<User> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAwareQueryResult<User> singleQuery(UserAwareQuery<User> query)
            throws QueryExecutionException {
        Id id = query.getOnlyId();
        if (!query.getContext().isAdminUser() && !id.equals(query.getContext()
                .getUser()
                .get()
                .getId())) {
            throw new ResourceForbiddenException();
        }
        Optional<User> user = userStore.userForId(id);
        if (user.isPresent()) {
            return UserAwareQueryResult.singleResult(user.get(), query.getContext());
        } else {
            throw new NotFoundException(id);
        }
    }

    private UserAwareQueryResult<User> multipleQuery(UserAwareQuery<User> query)
            throws QueryExecutionException {
        AttributeQuerySet operands = query.getOperands();
        // Can only return own profile if non admin user
        if (!query.getContext().isAdminUser()) {
            return usersQueryForIds(
                    query,
                    ImmutableList.of(query.getContext().getUser().get().getId())
            );
        }

        Iterable<Id> ids = Iterables.concat(operands.accept(new QueryVisitorAdapter<List<Id>>() {

            @Override
            public List<Id> visit(IdAttributeQuery query) {
                return query.getValue();
            }
        }));
        if (!Iterables.isEmpty(ids)) {
            return usersQueryForIds(query, ids);
        } else {
            return allUsersQuery(query);
        }

    }

    private UserAwareQueryResult<User> usersQueryForIds(UserAwareQuery<User> query,
            Iterable<Id> ids)
            throws QueryExecutionException {
        ListenableFuture<Resolved<User>> resolved = userStore.resolveIds(ids);
        Resolved<User> users = Futures.get(resolved, QueryExecutionException.class);
        return UserAwareQueryResult.listResult(users.getResources(), query.getContext());
    }

    private UserAwareQueryResult<User> allUsersQuery(UserAwareQuery<User> query) {
        return UserAwareQueryResult.listResult(userStore.allUsers(), query.getContext());
    }
}
