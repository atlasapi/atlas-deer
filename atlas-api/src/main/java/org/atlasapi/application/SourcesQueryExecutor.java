package org.atlasapi.application;

import java.util.List;

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
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class SourcesQueryExecutor implements UserAwareQueryExecutor<Publisher> {

    private final SourceIdCodec sourceIdCodec;

    public SourcesQueryExecutor(SourceIdCodec sourceIdCodec) {
        this.sourceIdCodec = sourceIdCodec;
    }

    @Override
    public UserAwareQueryResult<Publisher> execute(UserAwareQuery<Publisher> query)
            throws QueryExecutionException {
        return query.isListQuery() ? multipleQuery(query) : singleQuery(query);
    }

    private UserAwareQueryResult<Publisher> singleQuery(UserAwareQuery<Publisher> query)
            throws NotFoundException, ResourceForbiddenException {
        Optional<Publisher> source = sourceIdCodec.decode(query.getOnlyId());
        if (source.isPresent()) {
            if (!userManagesSource(source.get(), query)) {
                throw new ResourceForbiddenException();
            }
            return UserAwareQueryResult.singleResult(source.get(), query.getContext());
        } else {
            throw new NotFoundException(query.getOnlyId());
        }
    }

    private UserAwareQueryResult<Publisher> multipleQuery(UserAwareQuery<Publisher> query)
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
            return UserAwareQueryResult.listResult(sources, query.getContext());
        } else {
            return UserAwareQueryResult.listResult(
                    filterByUserViewable(sources, query),
                    query.getContext()
            );
        }
    }

    private boolean userManagesSource(Publisher source, UserAwareQuery<Publisher> query) {
        Optional<User> user = query.getContext().getUser();
        if (!user.isPresent()) {
            return false;
        } else {
            return user.get().is(Role.ADMIN) || user.get().getSources().contains(source);
        }
    }

    private Iterable<Publisher> filterByUserViewable(Iterable<Publisher> sources,
            UserAwareQuery<Publisher> query) {
        final User user = query.getContext().getUser().get();
        return Iterables.filter(sources, new Predicate<Publisher>() {

            @Override
            public boolean apply(@Nullable Publisher input) {
                return user.getSources().contains(input);
            }
        });
    }
}
