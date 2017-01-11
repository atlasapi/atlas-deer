package org.atlasapi.application.users;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class UsersQueryExecutorMultipleAccounts implements UserAccountsAwareQueryExecutor<User> {

    private final UserStore userStore;

    public UsersQueryExecutorMultipleAccounts(UserStore userStore) {
        this.userStore = userStore;
    }

    @Override
    public UserAccountsAwareQueryResult<User> execute(UserAccountsAwareQuery<User> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAccountsAwareQueryResult<User> singleQuery(UserAccountsAwareQuery<User> query)
            throws QueryExecutionException {
        Id id = query.getOnlyId();
        if (!query.getContext().isAdminUser() && !userAccessingHimself(query, id)) {
            throw new ResourceForbiddenException();
        }
        Optional<User> user = userStore.userForId(id);
        if (user.isPresent()) {
            return UserAccountsAwareQueryResult.singleResult(user.get(), query.getContext());
        } else {
            throw new NotFoundException(id);
        }
    }

    private boolean userAccessingHimself(UserAccountsAwareQuery<User> query, Id id) {
        return query.getContext()
                .getUserAccounts()
                .stream()
                .filter(user -> user.getId().equals(id))
                .findAny()
                .isPresent();
    }

    private UserAccountsAwareQueryResult<User> multipleQuery(UserAccountsAwareQuery<User> query)
            throws QueryExecutionException {
        AttributeQuerySet operands = query.getOperands();
        // Can only return own profile if non admin user
        if (!query.getContext().isAdminUser()) {
            List<Id> userIds = query.getContext().getUserAccounts()
                    .stream()
                    .map(User::getId)
                    .collect(Collectors.toList());
            return usersQueryForIds(
                    query,
                    userIds
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

    private UserAccountsAwareQueryResult<User> usersQueryForIds(UserAccountsAwareQuery<User> query,
            Iterable<Id> ids)
            throws QueryExecutionException {
        ListenableFuture<Resolved<User>> resolved = userStore.resolveIds(ids);
        Resolved<User> users = Futures.get(resolved, QueryExecutionException.class);
        return UserAccountsAwareQueryResult.listResult(users.getResources(), query.getContext());
    }

    private UserAccountsAwareQueryResult<User> allUsersQuery(UserAccountsAwareQuery<User> query) {
        return UserAccountsAwareQueryResult.listResult(userStore.allUsers(), query.getContext());
    }
}
