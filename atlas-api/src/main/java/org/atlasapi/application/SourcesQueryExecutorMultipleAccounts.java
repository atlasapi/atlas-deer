package org.atlasapi.application;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAccountsAwareQuery;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class SourcesQueryExecutorMultipleAccounts  implements
        UserAccountsAwareQueryExecutor<Publisher> {

    private final SourceIdCodec sourceIdCodec;

    public SourcesQueryExecutorMultipleAccounts(SourceIdCodec sourceIdCodec) {
        this.sourceIdCodec = sourceIdCodec;
    }

    @Override
    public UserAccountsAwareQueryResult<Publisher> execute(UserAccountsAwareQuery<Publisher> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAccountsAwareQueryResult<Publisher> singleQuery(UserAccountsAwareQuery<Publisher> query)
            throws NotFoundException, ResourceForbiddenException {
        Optional<Publisher> source = sourceIdCodec.decode(query.getOnlyId());
        if (source.isPresent()) {
            if (!userManagesSource(source.get(), query)) {
                throw new ResourceForbiddenException();
            }
            return UserAccountsAwareQueryResult.singleResult(source.get(), query.getContext());
        } else {
            throw new NotFoundException(query.getOnlyId());
        }
    }

    private UserAccountsAwareQueryResult<Publisher> multipleQuery(UserAccountsAwareQuery<Publisher> query)
            throws NotFoundException {
        AttributeQuerySet operands = query.getOperands();

        Iterable<Publisher> requestedSources = Iterables.concat(operands.accept(new QueryVisitorAdapter<List<Publisher>>() {

            @Override
            public List<Publisher> visit(IdAttributeQuery query) {
                return Lists.transform(query.getValue(), new Function<Id, Publisher>() {

                    @Override
                    public Publisher apply(Id input) {
                        return sourceIdCodec.decode(input).get();
                    }
                });
            }
        }));

        Iterable<Publisher> sources = null;
        if (Iterables.isEmpty(requestedSources)) {
            sources = Publisher.all();
        } else {
            sources = requestedSources;
        }

        if (query.getContext().isAdminUser()) {
            return UserAccountsAwareQueryResult.listResult(sources, query.getContext());
        } else {
            return UserAccountsAwareQueryResult.listResult(
                    filterByUserViewable(sources, query),
                    query.getContext()
            );
        }
    }

    private boolean userManagesSource(Publisher source, UserAccountsAwareQuery<Publisher> query) {
        Set<User> userAccounts = query.getContext().getUserAccounts();
        return userAccounts.stream()
                .filter(user -> user.is(Role.ADMIN) || user.getSources().contains(source))
                .findAny()
                .isPresent();
    }

    private Iterable<Publisher> filterByUserViewable(Iterable<Publisher> sources,
            UserAccountsAwareQuery<Publisher> query) {
        final Set<User> userAccounts = query.getContext().getUserAccounts();
        return Iterables.filter(sources, new Predicate<Publisher>() {

            @Override
            public boolean apply(@Nullable Publisher input) {
                return userAccounts.stream().filter(user -> user.getSources().contains(input)).findAny().isPresent();
            }
        });
    }
}
