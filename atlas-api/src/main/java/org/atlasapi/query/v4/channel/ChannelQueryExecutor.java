package org.atlasapi.query.v4.channel;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import com.metabroadcast.common.base.MoreOrderings;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryExecutor implements QueryExecutor<Channel> {

    private static final String TITLE = "title";
    private static final String TITLE_REVERSE = "title.reverse";

    private Ordering<? super Channel> ordering(String orderBy) {
        if (!Strings.isNullOrEmpty(orderBy)) {
            if (orderBy.equals(TITLE)) {
                return MoreOrderings.transformingOrdering(TO_ORDERING_TITLE);
            } else if (orderBy.equals(TITLE_REVERSE)) {
                return MoreOrderings.transformingOrdering(
                        TO_ORDERING_TITLE,
                        Ordering.<String>natural().reverse()
                );
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
    public QueryResult<Channel> execute(@Nonnull Query<Channel> query)
            throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private QueryResult<Channel> executeSingleQuery(final Query<Channel> query)
            throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        resolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        new Function<Resolved<Channel>, QueryResult<Channel>>() {

                            @Override
                            public QueryResult<Channel> apply(Resolved<Channel> input) {
                                if (input.getResources().isEmpty()) {
                                    throw new UncheckedQueryExecutionException(new NotFoundException(
                                            query.getOnlyId()));
                                }
                                return QueryResult.singleResult(
                                        input.getResources().first().get(),
                                        query.getContext()
                                );
                            }
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );
    }

    private QueryResult<Channel> executeListQuery(final Query<Channel> query)
            throws QueryExecutionException {
        ChannelQuery.Builder channelQueryBuilder = ChannelQuery.builder();
        Ordering<? super Channel> ordering = Ordering.allEqual();
        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            switch (attributeQuery.getAttributeName()) {
            case Attributes.AVAILABLE_FROM_PARAM:
                channelQueryBuilder.withAvailableFrom((Publisher) attributeQuery.getValue().get(0));
                break;
            case Attributes.BROADCASTER_PARAM:
                channelQueryBuilder.withBroadcaster((Publisher) attributeQuery.getValue().get(0));
                break;
            case Attributes.MEDIA_TYPE_PARAM:
                channelQueryBuilder.withMediaType(
                        org.atlasapi.media.entity.MediaType.valueOf(
                                attributeQuery.getValue().get(0).toString().toUpperCase()
                        )
                );
            case Attributes.ORDER_BY_PARAM:
                ordering = ordering(attributeQuery.getValue().get(0).toString());
                break;
            case Attributes.ADVERTISED_FROM_PARAM:
                channelQueryBuilder.withAdvertisedOn(DateTime.now(DateTimeZone.UTC));
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Incorrect query string parameter: %s",
                                attributeQuery.getAttributeName()
                        )
                );
            }
        }

        ListenableFuture<Resolved<Channel>> resolvingChannels = resolver.resolveChannels(
                channelQueryBuilder.build());
        ListenableFuture<FluentIterable<Channel>> resolvedIterable =
                Futures.transform(
                        resolvingChannels,
                        new Function<Resolved<Channel>, FluentIterable<Channel>>() {

                            @Override
                            public FluentIterable<Channel> apply(
                                    @Nullable Resolved<Channel> resolved) {
                                return resolved.getResources();
                            }
                        }
                );
        final ImmutableList<Channel> channels;
        try {
            channels = ordering.immutableSortedCopy(Futures.get(
                    resolvedIterable,
                    1,
                    TimeUnit.MINUTES,
                    QueryExecutionException.class
            ));
        } catch (Exception e) {
            throw new QueryExecutionException(e);
        }

        ImmutableList<Channel> filteredChannels = ImmutableList.copyOf(Iterables.filter(
                channels,
                input -> {
                    return query.getContext()
                            .getApplicationSources()
                            .isReadEnabled(input.getBroadcaster());
                }
        ));
        return QueryResult.listResult(
                query.getContext().getSelection().get().applyTo(
                        filteredChannels
                ),
                query.getContext(),
                filteredChannels.size()
        );
    }
}
