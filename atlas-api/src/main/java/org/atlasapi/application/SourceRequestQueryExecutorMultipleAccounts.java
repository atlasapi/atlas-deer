package org.atlasapi.application;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.application.users.User;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class SourceRequestQueryExecutorMultipleAccounts implements
        UserAccountsAwareQueryExecutor<SourceRequest> {

    private final SourceRequestStore requestStore;
    private static final QueryVisitorAdapter<Publisher> PUBLISHERS_VISITOR = new QueryVisitorAdapter<Publisher>() {

        @Override
        public Publisher visit(EnumAttributeQuery<?> query) {
            if (query.getAttributeName().equals(Attributes.SOURCE_REQUEST_SOURCE.externalName())) {
                return (Publisher) Iterables.getOnlyElement(query.getValue());
            } else {
                return null;
            }
        }
    };

    public SourceRequestQueryExecutorMultipleAccounts(SourceRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Override
    public UserAccountsAwareQueryResult<SourceRequest> execute(UserAccountsAwareQuery<SourceRequest> query)
            throws QueryExecutionException {
        AttributeQuerySet operands = query.getOperands();
        Set<User> userAccounts = query.getContext().getUserAccounts();

        List<Publisher> source = operands.accept(PUBLISHERS_VISITOR);
        Iterable<SourceRequest> results;

        if (source.isEmpty() && query.getContext().isAdminUser()) {
            results = requestStore.all();
        } else if (source.isEmpty() && !query.getContext().isAdminUser()) {
            Set<Id> applicationIds = userAccounts
                    .stream()
                    .map(User::getApplicationIds)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            results = requestStore.sourceRequestsForApplicationIds(
                    applicationIds);
        } else if (!source.isEmpty() && query.getContext().isAdminUser()) {
            results = requestStore.sourceRequestsFor(source.get(0));
        } else {
            results = filterRequestsByUserApplications(
                    requestStore.sourceRequestsFor(source.get(0)),
                    userAccounts
            );
        }

        return UserAccountsAwareQueryResult.listResult(results, query.getContext());
    }

    private Iterable<SourceRequest> filterRequestsByUserApplications(
            Iterable<SourceRequest> sourceRequests, final Set<User> user) {
        return Iterables.filter(sourceRequests, new Predicate<SourceRequest>() {

            @Override
            public boolean apply(@Nullable SourceRequest input) {
                return user.stream()
                        .filter(user->user.getApplicationIds().contains(input.getId()))
                        .findAny()
                        .isPresent();
            }
        });
    }
}
