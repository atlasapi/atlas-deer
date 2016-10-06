package org.atlasapi.query.v4.channel;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import com.metabroadcast.common.base.MoreOrderings;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.promise.Promise;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
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

public class ChannelQueryExecutor implements QueryExecutor<ResolvedChannel> {

    private static final String TITLE = "title";
    private static final String TITLE_REVERSE = "title.reverse";

    private final ChannelResolver channelResolver;
    private final ChannelGroupResolver channelGroupResolver;

    private ChannelQueryExecutor(
            ChannelResolver channelResolver,
            ChannelGroupResolver channelGroupResolver
    ) {
        this.channelResolver = checkNotNull(channelResolver);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
    }

    public static ChannelQueryExecutor create(
            ChannelResolver channelResolver,
            ChannelGroupResolver channelGroupResolver
    ) {
        return new ChannelQueryExecutor(channelResolver, channelGroupResolver);
    }

    @Override
    public QueryResult<ResolvedChannel> execute(Query<ResolvedChannel> query) throws QueryExecutionException {
        return query.isListQuery()
               ? executeListQuery(query)
               : executeSingleQuery(query);
    }

    private QueryResult<ResolvedChannel> executeSingleQuery(Query<ResolvedChannel> query)
            throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        channelResolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        (Function<Resolved<Channel>, QueryResult<ResolvedChannel>>) input -> {
                            if (input.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(
                                        new NotFoundException(query.getOnlyId())
                                );
                            }

                            ResolvedChannel resolvedChannel =
                                    resolveAnnotationData(
                                            query.getContext(),
                                            input.getResources().first().get()
                                    );

                            return QueryResult.singleResult(
                                    resolvedChannel,
                                    query.getContext()
                            );
                        }
                ),
                1,
                TimeUnit.MINUTES,
                QueryExecutionException.class
        );
    }

    private QueryResult<ResolvedChannel> executeListQuery(Query<ResolvedChannel> query)
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
                .collect(MoreCollectors.toImmutableList());

        ImmutableList<Channel> selectedChannels = query
                .getContext()
                .getSelection()
                .get()
                .applyTo(filteredChannels);

        ImmutableList<ResolvedChannel> resolvedChannels =
                selectedChannels.stream()
                        .map(channel -> resolveAnnotationData(query.getContext(), channel))
                .collect(MoreCollectors.toImmutableList());

        return QueryResult.listResult(
                resolvedChannels,
                query.getContext(),
                filteredChannels.size()
        );
    }

    private FluentIterable<Channel> getChannels(ChannelQuery channelQuery)
            throws QueryExecutionException {
        ListenableFuture<Resolved<Channel>> resolvingChannels = channelResolver.resolveChannels(
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

    private ResolvedChannel resolveAnnotationData(QueryContext ctxt, Channel channel) {
        ResolvedChannel.Builder resolvedChannelBuilder =
                ResolvedChannel.builder(channel);

        resolvedChannelBuilder.withChannelGroupSummaries(
                contextHasAnnotation(ctxt, Annotation.CHANNEL_GROUPS_SUMMARY) ?
                    resolveChannelGroupSummaries(channel) :
                    Optional.absent()
        );

        resolvedChannelBuilder.withParentChannel(
                contextHasAnnotation(ctxt, Annotation.PARENT) ?
                    resolveParentChannel(channel) :
                    Optional.absent()
        );

        resolvedChannelBuilder.withChannelVariations(
                contextHasAnnotation(ctxt, Annotation.VARIATIONS) ?
                    resolveChannelVariations(channel) :
                    Optional.absent()
        );

        return resolvedChannelBuilder.build();
    }

    private Optional<List<ChannelGroupSummary>> resolveChannelGroupSummaries(Channel channel) {

        ImmutableList<Id> channelGroupIds = channel.getChannelGroups()
                .stream()
                .map(cg -> cg.getChannelGroup().getId())
                .collect(MoreCollectors.toImmutableList());

        Iterable<ChannelGroup<?>> channelGroups =
                Promise.wrap(channelGroupResolver.resolveIds(channelGroupIds))
                        .then(Resolved::getResources)
                        .get(1, TimeUnit.MINUTES);

        return Optional.of(StreamSupport.stream(channelGroups.spliterator(), false)
                .map(ChannelGroup::toSummary)
                .collect(MoreCollectors.toImmutableList()));

    }

    private Optional<Channel> resolveParentChannel(Channel channel) {

        return Optional.of(Promise.wrap(channelResolver.resolveIds(
                ImmutableList.of(channel.getParent().getId())))
                .then(Resolved::getResources)
                .then(FluentIterable::first)
                .then(Optional::get)
                .get(1, TimeUnit.MINUTES));
    }

    private Optional<Iterable<Channel>> resolveChannelVariations(Channel channel) {

        Iterable<Id> ids = Iterables.transform(channel.getVariations(), ResourceRef::getId);

        return Optional.of(Promise.wrap(channelResolver.resolveIds(ids))
        .then(Resolved::getResources)
        .get(1, TimeUnit.MINUTES));

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

    private boolean contextHasAnnotation(QueryContext ctxt, Annotation annotation) {

        return (!Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
                &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        ).contains(annotation.toKey()));
    }
}
