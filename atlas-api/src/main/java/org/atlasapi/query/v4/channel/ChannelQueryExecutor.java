package org.atlasapi.query.v4.channel;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.MoreOrderings;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.MediaType;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.QueryNode;
import org.atlasapi.criteria.QueryNodeVisitor;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryExecutor implements QueryExecutor<Channel> {

    private static final String TITLE = "title";
    private static final String TITLE_REVERSE = "title.reverse";
    private Ordering<? super Channel> ordering(String orderBy) {
        if (!Strings.isNullOrEmpty(orderBy)) {
            if (orderBy.equals(TITLE)) {
                return MoreOrderings.transformingOrdering(TO_ORDERING_TITLE);
            } else if (orderBy.equals(TITLE_REVERSE)) {
                return MoreOrderings.transformingOrdering(TO_ORDERING_TITLE, Ordering.<String>natural().reverse());
            }
        }
        return Ordering.allEqual();

    }

    private static final Function<Channel, String> TO_ORDERING_TITLE = new Function<Channel, String>() {
        @Override
        public String apply(Channel input) {
            return Strings.nullToEmpty(input.getTitle());
        }
    };

    private final ChannelResolver resolver;

    public ChannelQueryExecutor(ChannelResolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    @Nonnull
    @Override
    public QueryResult<Channel> execute(@Nonnull Query<Channel> query) throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private QueryResult<Channel> executeSingleQuery(final Query<Channel> query) throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        resolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        new Function<Resolved<Channel>, QueryResult<Channel>>() {
                            @Override
                            public QueryResult<Channel> apply(Resolved<Channel> input) {
                                if (input.getResources().isEmpty()) {
                                    throw new UncheckedQueryExecutionException(new NotFoundException(query.getOnlyId()));
                                }
                                return QueryResult.singleResult(input.getResources().first().get(), query.getContext());
                            }
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );
    }

    private QueryResult<Channel> executeListQuery(final Query<Channel> query) throws QueryExecutionException {
        ChannelQuery.Builder channelQueryBuilder = ChannelQuery.builder();
        Ordering<? super Channel> ordering = Ordering.allEqual();
        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            switch (attributeQuery.getAttributeName()) {
                case "available_from":
                    channelQueryBuilder.withAvailableFrom((Publisher) attributeQuery.getValue().get(0));
                    break;
                case "broadcaster":
                    channelQueryBuilder.withBroadcaster((Publisher) attributeQuery.getValue().get(0));
                    break;
                case "media_type":
                    channelQueryBuilder.withMediaType(
                            org.atlasapi.media.entity.MediaType.valueOf(
                                    attributeQuery.getValue().get(0).toString().toUpperCase()
                            )
                    );
                    break;
                case "order_by":
                    ordering = ordering(attributeQuery.getValue().get(0).toString());
                    break;
                default:
            }
        }

        final Iterable<Channel> channels =
                ordering.immutableSortedCopy(
                        Futures.get(
                                Futures.transform(
                                        resolver.resolveChannels(channelQueryBuilder.build()),
                                        new Function<Resolved<Channel>, Iterable<Channel>>() {
                                            @Override
                                            public Iterable<Channel> apply(Resolved<Channel> input) {
                                                return input.getResources();
                                            }
                                        }),
                                1, TimeUnit.MINUTES,
                                QueryExecutionException.class
                        )
                );

        return QueryResult.listResult(
                query.getContext().getSelection().get().applyTo(
                        Iterables.filter(
                                channels,
                                new Predicate<Channel>() {
                                    @Override
                                    public boolean apply(@Nullable Channel input) {
                                        return query.getContext().getApplicationSources().isReadEnabled(input.getBroadcaster());
                                    }
                                }
                        )
                ),
                query.getContext()
        );
    }
}
