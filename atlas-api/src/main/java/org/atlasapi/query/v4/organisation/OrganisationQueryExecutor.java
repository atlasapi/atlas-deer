package org.atlasapi.query.v4.organisation;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.exceptions.UncheckedQueryExecutionException;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class OrganisationQueryExecutor implements QueryExecutor<Organisation> {

    private final OrganisationResolver resolver;

    public OrganisationQueryExecutor(OrganisationResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public QueryResult<Organisation> execute(Query<Organisation> query)
            throws QueryExecutionException {
        try {
            return Futures.getChecked(
                    executeQuery(query),
                    QueryExecutionException.class,
                    1,
                    TimeUnit.MINUTES
            );
        } catch (UncheckedQueryExecutionException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private ListenableFuture<QueryResult<Organisation>> executeQuery(
            final Query<Organisation> query) {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private ListenableFuture<QueryResult<Organisation>> executeSingleQuery(
            final Query<Organisation> query) {
        Id id = query.getOnlyId();
        return Futures.transform(
                resolve(id),
                (Resolved<Organisation> input) -> {
                    List<Organisation> organisationList = input.getResources().toList();
                    if (organisationList.isEmpty()) {
                        throw new UncheckedQueryExecutionException(new NotFoundException(id));
                    }
                    Organisation resource = organisationList.get(0);
                    return QueryResult.singleResult(resource, query.getContext());
                }
        );

    }

    private ListenableFuture<QueryResult<Organisation>> executeListQuery(
            final Query<Organisation> query) {
        throw new UnsupportedOperationException();
    }

    private ListenableFuture<Resolved<Organisation>> resolve(Id id) {
        return resolver.resolveIds(ImmutableSet.of(id));
    }
}
