package org.atlasapi.query.v4.channel;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.base.MoreOrderings;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.promise.Promise;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.query.common.exceptions.UncheckedQueryExecutionException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    public QueryResult<ResolvedChannel> execute(
            Query<ResolvedChannel> query
    ) throws QueryExecutionException {
        try {
            return query.isListQuery()
                    ? executeListQuery(query)
                    : executeSingleQuery(query);
        } catch (UncheckedExecutionException | UncheckedQueryExecutionException ee) {
            for (Throwable throwable : Throwables.getCausalChain(ee)) {
                if (throwable instanceof NotFoundException) {
                    throw (NotFoundException) throwable;
                }
            }
            Throwables.propagateIfInstanceOf(
                    Throwables.getRootCause(ee),
                    QueryExecutionException.class
            );
            throw Throwables.propagate(ee);
        }
    }

    private QueryResult<ResolvedChannel> executeSingleQuery(Query<ResolvedChannel> query)
            throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        channelResolver.resolveIds(
                                ImmutableSet.of(query.getOnlyId()),
                                Boolean.parseBoolean(
                                        query.getContext().getRequest().getParameter(
                                                Attributes.REFRESH_CACHE.externalName()
                                        )
                                )
                        ),
                        (Function<Resolved<Channel>, QueryResult<ResolvedChannel>>) input -> {
                            if (input.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(
                                        new NotFoundException(query.getOnlyId())
                                );
                            }

                            Channel channel = input.getResources().first().get();

                            if (!query.getContext()
                                    .getApplication()
                                    .getConfiguration()
                                    .isReadEnabled(channel.getSource())
                            ) {
                                throw new UncheckedQueryExecutionException(
                                        new NotFoundException(query.getOnlyId())
                                );
                            }

                            ResolvedChannel resolvedChannel =
                                    resolveAnnotationData(
                                            query.getContext(),
                                            channel
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
            String attributeName = attributeQuery.getAttributeName();
            Object attributeValue = attributeQuery.getValue().get(0);

            if (Attributes.ALIASES_NAMESPACE.externalName().equals(attributeName)) {
                channelQueryBuilder.withAliasNamespace((String) attributeValue);
            } else if (Attributes.ALIASES_VALUE.externalName().equals(attributeName)) {
                channelQueryBuilder.withAliasValue((String) attributeValue);
            } else if (Attributes.AVAILABLE_FROM.externalName().equals(attributeName)) {
                channelQueryBuilder.withAvailableFrom((Publisher) attributeValue);
            } else if (Attributes.SOURCE.externalName().equals(attributeName)) {
                channelQueryBuilder.withPublisher((Publisher) attributeValue);
            } else if (Attributes.BROADCASTER.externalName().equals(attributeName)) {
                channelQueryBuilder.withBroadcaster((Publisher) attributeValue);
            } else if (Attributes.MEDIA_TYPE.externalName().equals(attributeName)) {
                channelQueryBuilder.withMediaType(
                        MediaType.valueOf(
                                attributeValue.toString().toUpperCase()
                        )
                );
            } else if (Attributes.ORDER_BY.externalName().equals(attributeName)) {
                ordering = ordering(attributeValue.toString());
            } else if (Attributes.ADVERTISED_ON.externalName().equals(attributeName)) {
                channelQueryBuilder.withAdvertisedOn(DateTime.now(DateTimeZone.UTC));
            }
        }

        Iterable<Channel> channels = getChannels(channelQueryBuilder.build());

        ImmutableList<Channel> filteredChannels = ordering.immutableSortedCopy(channels)
                .stream()
                .filter(
                        input -> query
                                .getContext()
                                .getApplication()
                                .getConfiguration()
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

    protected boolean queryHasAliasAttributesOnly(ChannelQuery channelQuery) {

        return !channelQuery.getAdvertisedOn().isPresent() &&
                !channelQuery.getAvailableFrom().isPresent() &&
                !channelQuery.getBroadcaster().isPresent() &&
                !channelQuery.getChannelGroups().isPresent() &&
                !channelQuery.getGenres().isPresent() &&
                !channelQuery.getMediaType().isPresent() &&
                !channelQuery.getPublisher().isPresent() &&
                !channelQuery.getUri().isPresent() &&
                channelQuery.getAliasNamespace().isPresent() &&
                channelQuery.getAliasValue().isPresent();
    }

    private Iterable<Channel> getChannels(ChannelQuery channelQuery)
            throws QueryExecutionException {

        ListenableFuture<Resolved<Channel>> resolvingChannels;

        if (queryHasAliasAttributesOnly(channelQuery)) {
            resolvingChannels = channelResolver.resolveChannelsWithAliases(channelQuery);
        } else {
            resolvingChannels = channelResolver.resolveChannels(channelQuery);
        }

        ListenableFuture<FluentIterable<Channel>> resolvedIterable = Futures.transform(
                resolvingChannels,
                (Function<Resolved<Channel>, FluentIterable<Channel>>) Resolved::getResources
        );

        try {
            return Futures.getChecked(
                    resolvedIterable,
                    QueryExecutionException.class,
                    1,
                    TimeUnit.MINUTES
            );
        } catch (Exception e) {
            throw new QueryExecutionException(e);
        }
    }

    private ResolvedChannel resolveAnnotationData(QueryContext ctxt, Channel channel) {
        ResolvedChannel.Builder resolvedChannelBuilder = ResolvedChannel.builder(channel);

        if (contextHasAnnotation(ctxt, Annotation.CHANNEL_GROUPS_SUMMARY)) {
            resolvedChannelBuilder.withChannelGroupSummaries(resolveChannelGroupSummaries(ctxt, channel));
        }

        if (contextHasAnnotation(ctxt, Annotation.PARENT)) {
            resolvedChannelBuilder.withParentChannel(resolveParentChannel(channel));
        }

        if (contextHasAnnotation(ctxt, Annotation.VARIATIONS)) {
            resolvedChannelBuilder.withChannelVariations(resolveChannelVariations(channel));
        }

        if (ctxt.getApplication().getConfiguration().isPrecedenceEnabled()) {
            resolvedChannelBuilder.withResolvedEquivalents(resolveEquivalents(channel));
        }

        return resolvedChannelBuilder.build();
    }

    private List<ChannelGroupSummary> resolveChannelGroupSummaries(QueryContext ctxt, Channel channel) {

        ApplicationConfiguration conf = ctxt.getApplication().getConfiguration();

        Iterable<ChannelGroup<?>> channelGroups =
                Promise.wrap(channelGroupResolver.resolveIds(
                        channel.getChannelGroups()
                                .stream()
                                .filter(cg -> conf.isReadEnabled(cg.getChannelGroup().getSource()))
                                .map(cg -> cg.getChannelGroup().getId())
                                .collect(Collectors.toList())))
                        .then(Resolved::getResources)
                        .get(1, TimeUnit.MINUTES);

        return StreamSupport.stream(channelGroups.spliterator(), false)
                .map(ChannelGroup::toSummary)
                .collect(MoreCollectors.toImmutableList());

    }

    private Channel resolveParentChannel(Channel channel) {

        return Promise.wrap(channelResolver.resolveIds(
                ImmutableList.of(channel.getParent().getId())))
                .then(Resolved::getResources)
                .then(FluentIterable::first)
                .then(com.google.common.base.Optional::get)
                .get(1, TimeUnit.MINUTES);
    }

    private Iterable<Channel> resolveChannelVariations(Channel channel) {

        Iterable<Id> ids = Iterables.transform(channel.getVariations(), ChannelRef::getId);

        return Promise.wrap(channelResolver.resolveIds(ids))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES);

    }

    private Iterable<Channel> resolveEquivalents(Channel channel) {

        Iterable<Id> ids = Iterables.transform(channel.getSameAs(), ChannelEquivRef::getId);

        return Promise.wrap(channelResolver.resolveIds(ids))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES);
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
