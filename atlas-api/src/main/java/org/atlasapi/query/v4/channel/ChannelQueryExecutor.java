package org.atlasapi.query.v4.channel;

import java.util.concurrent.TimeUnit;

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
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.base.MoreOrderings;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryExecutor implements QueryExecutor<Channel> {

    private static final String TITLE = "title";
    private static final String TITLE_REVERSE = "title.reverse";

    private final ChannelResolver resolver;

    private ChannelQueryExecutor(ChannelResolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    public static ChannelQueryExecutor create(ChannelResolver channelResolver) {
        return new ChannelQueryExecutor(channelResolver);
    }

    @Override
    public QueryResult<Channel> execute(Query<Channel> query) throws QueryExecutionException {
        return query.isListQuery()
               ? executeListQuery(query)
               : executeSingleQuery(query);
    }

    private QueryResult<Channel> executeSingleQuery(Query<Channel> query)
            throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        resolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        (Function<Resolved<Channel>, QueryResult<Channel>>) input -> {
                            if (input.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(
                                        new NotFoundException(query.getOnlyId())
                                );
                            }
                            return QueryResult.singleResult(
                                    input.getResources().first().get(),
                                    query.getContext()
                            );
                        }
                ),
                1,
                TimeUnit.MINUTES,
                QueryExecutionException.class
        );
    }

    private QueryResult<Channel> executeListQuery(Query<Channel> query)
            throws QueryExecutionException {

        ChannelQuery.Builder channelQueryBuilder = ChannelQuery.builder();
        Ordering<? super Channel> ordering = Ordering.allEqual();

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            Object attributeValue = attributeQuery.getValue().get(0);

            switch (attributeQuery.getAttributeName()) {
            case Attributes.AVAILABLE_FROM_PARAM:
                channelQueryBuilder.withAvailableFrom((Publisher) attributeValue);
                break;
            case Attributes.BROADCASTER_PARAM:
                channelQueryBuilder.withBroadcaster((Publisher) attributeValue);
                break;
            case Attributes.MEDIA_TYPE_PARAM:
                channelQueryBuilder.withMediaType(
                        org.atlasapi.media.entity.MediaType.valueOf(
                                attributeValue.toString().toUpperCase()
                        )
                );
            case Attributes.ORDER_BY_PARAM:
                ordering = ordering(attributeValue.toString());
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

        FluentIterable<Channel> channels = getChannels(channelQueryBuilder.build());

        ImmutableList<Channel> filteredChannels = ordering.immutableSortedCopy(channels)
                .stream()
                .filter(
                        input -> query
                                .getContext()
                                .getApplicationSources()
                                .isReadEnabled(input.getSource())
                )
                .collect(ImmutableCollectors.toList());

        ImmutableList<Channel> selectedChannels = query
                .getContext()
                .getSelection()
                .get()
                .applyTo(filteredChannels);

        return QueryResult.listResult(
                selectedChannels,
                query.getContext(),
                filteredChannels.size()
        );
    }

    private FluentIterable<Channel> getChannels(ChannelQuery channelQuery)
            throws QueryExecutionException {
        ListenableFuture<Resolved<Channel>> resolvingChannels = resolver.resolveChannels(
                channelQuery
        );

        ListenableFuture<FluentIterable<Channel>> resolvedIterable = Futures.transform(
                resolvingChannels,
                (Function<Resolved<Channel>, FluentIterable<Channel>>) Resolved::getResources
        );

        try {
            return Futures.get(
                    resolvedIterable,
                    1,
                    TimeUnit.MINUTES,
                    QueryExecutionException.class
            );
        } catch (Exception e) {
            throw new QueryExecutionException(e);
        }
    }

    private Ordering<? super Channel> ordering(String orderBy) {
        Ordering<? super Channel> ordering = Ordering.allEqual();

        if (Strings.isNullOrEmpty(orderBy)) {
            return ordering;
        }

        switch (orderBy) {
        case TITLE:
            ordering = MoreOrderings.transformingOrdering(
                    input -> Strings.nullToEmpty(input.getTitle())
            );
            break;
        case TITLE_REVERSE:
            ordering = MoreOrderings.transformingOrdering(
                    input -> Strings.nullToEmpty(input.getTitle()),
                    Ordering.<String>natural().reverse()
            );
            break;
        default:
            break;
        }

        return ordering;
    }
}
