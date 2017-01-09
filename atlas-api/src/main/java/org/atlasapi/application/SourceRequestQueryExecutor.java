package org.atlasapi.application;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.application.users.User;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class SourceRequestQueryExecutor implements UserAwareQueryExecutor<SourceRequest> {

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

    public SourceRequestQueryExecutor(SourceRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Override
    public UserAwareQueryResult<SourceRequest> execute(UserAwareQuery<SourceRequest> query)
            throws QueryExecutionException {
        AttributeQuerySet operands = query.getOperands();
        User user = query.getContext().getUser().get();

        List<Publisher> source = operands.accept(PUBLISHERS_VISITOR);
        Iterable<SourceRequest> results;

        if (source.isEmpty() && query.getContext().isAdminUser()) {
            results = requestStore.all();
        } else if (source.isEmpty() && !query.getContext().isAdminUser()) {
            results = requestStore.sourceRequestsForApplicationIds(
                    user.getApplicationIds());
        } else if (!source.isEmpty() && query.getContext().isAdminUser()) {
            results = requestStore.sourceRequestsFor(source.get(0));
        } else {
            results = filterRequestsByUserApplications(
                    requestStore.sourceRequestsFor(source.get(0)),
                    user
            );
        }

        return UserAwareQueryResult.listResult(results, query.getContext());
    }

    private Iterable<SourceRequest> filterRequestsByUserApplications(
            Iterable<SourceRequest> sourceRequests, final User user) {
        return Iterables.filter(sourceRequests, new Predicate<SourceRequest>() {

            @Override
            public boolean apply(@Nullable SourceRequest input) {
                return user.getApplicationIds().contains(input.getAppId());
            }
        });
    }
}
