package org.atlasapi.query.v4.channelgroup;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import javax.annotation.Nonnull;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryExecutor implements QueryExecutor<ChannelGroup> {

    private final ChannelGroupResolver resolver;

    public ChannelGroupQueryExecutor(ChannelGroupResolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    @Nonnull
    @Override
    public QueryResult<ChannelGroup> execute(@Nonnull Query<ChannelGroup> query) throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private QueryResult<ChannelGroup> executeSingleQuery(final Query<ChannelGroup> query) throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        resolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        new Function<Resolved<ChannelGroup>, QueryResult<ChannelGroup>>() {
                            @Override
                            public QueryResult<ChannelGroup> apply(Resolved<ChannelGroup> input) {
                                if (input.getResources().isEmpty()) {
                                    throw new UncheckedQueryExecutionException(new NotFoundException(query.getOnlyId()));
                                }
                                return QueryResult.singleResult(input.getResources().first().get(), query.getContext());
                            }
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );
    }

    private QueryResult<ChannelGroup> executeListQuery(final Query<ChannelGroup> query) throws QueryExecutionException {
        Iterable<ChannelGroup> channelGroups = Futures.get(
                Futures.transform(
                        resolver.allChannels(),
                        new Function<Resolved<ChannelGroup>, Iterable<ChannelGroup>>() {
                            @Override
                            public Iterable<ChannelGroup> apply(Resolved<ChannelGroup> input) {
                                return input.getResources();
                            }
                        }),
                1, TimeUnit.MINUTES,
                QueryExecutionException.class
        );

        return QueryResult.listResult(
                query.getContext().getSelection().get().applyTo(
                        Iterables.filter(channelGroups, new Predicate<ChannelGroup>() {
                            @Override
                            public boolean apply(ChannelGroup input) {
                                return query.getContext().getApplicationSources().isReadEnabled(input.getPublisher());
                            }
                        })),
                query.getContext()
        );
    }
}
