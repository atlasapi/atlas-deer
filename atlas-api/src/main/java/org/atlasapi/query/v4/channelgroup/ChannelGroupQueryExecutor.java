package org.atlasapi.query.v4.channelgroup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.channel.Platform;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryExecutor;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.UncheckedQueryExecutionException;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryExecutor implements QueryExecutor<ResolvedChannelGroup> {

    private final ChannelGroupResolver resolver;

    public ChannelGroupQueryExecutor(ChannelGroupResolver resolver) {
        this.resolver = checkNotNull(resolver);
    }

    @Nonnull
    @Override
    public QueryResult<ResolvedChannelGroup> execute(@Nonnull Query<ResolvedChannelGroup> query)  //resolvedchannelgroup
            throws QueryExecutionException {
        return query.isListQuery() ? executeListQuery(query) : executeSingleQuery(query);
    }

    private QueryResult<ResolvedChannelGroup> executeSingleQuery(final Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        return Futures.get(
                Futures.transform(
                        resolver.resolveIds(ImmutableSet.of(query.getOnlyId())),
                        (Resolved<ChannelGroup<?>> input) -> {
                            if (input.getResources().isEmpty()) {
                                throw new UncheckedQueryExecutionException(new NotFoundException(
                                        query.getOnlyId()));
                            }

                            ResolvedChannelGroup resolvedChannelGroup =
                                    resolveAnnotationData(
                                            query.getContext(),
                                            input.getResources().first().get()
                                    );

                            return QueryResult.singleResult(
                                    resolvedChannelGroup,
                                    query.getContext()
                            );
                        }
                ), 1, TimeUnit.MINUTES, QueryExecutionException.class
        );                                                                              // MESS THIS UP
    }

    private QueryResult<ResolvedChannelGroup> executeListQuery(final Query<ResolvedChannelGroup> query)
            throws QueryExecutionException {
        Iterable<ChannelGroup<?>> channelGroups = Futures.get(
                Futures.transform(
                        resolver.allChannels(),
                        (Resolved<ChannelGroup<?>> input) -> {
                            return input.getResources();
                        }
                ),
                1, TimeUnit.MINUTES,
                QueryExecutionException.class
        );

        for (AttributeQuery<?> attributeQuery : query.getOperands()) {
            if (attributeQuery.getAttributeName()
                    .equals(Attributes.CHANNEL_GROUP_TYPE.externalName())) {
                final String channelGroupType = attributeQuery.getValue().get(0).toString();
                channelGroups = Iterables.filter(
                        channelGroups,
                        channelGroup -> {
                            return channelGroupType.equals(channelGroup.getType());
                        }
                );

            }
        }
        ImmutableList<ChannelGroup<?>> filteredChannelGroups = ImmutableList.copyOf(Iterables.filter(
                channelGroups,
                input -> {
                    return query.getContext()
                            .getApplicationSources()
                            .isReadEnabled(input.getSource());
                }
        ));

        ImmutableList<ChannelGroup<?>> selectedChannelGroups =
                query.getContext()
                        .getSelection()
                        .get()
                        .applyTo(filteredChannelGroups);

        ImmutableList<ResolvedChannelGroup> resolvedChannelGroups =
                selectedChannelGroups.stream()
                        .map(channelGroup ->
                                resolveAnnotationData(query.getContext(), channelGroup)
                        )
                        .collect(MoreCollectors.toImmutableList());


        return QueryResult.listResult(
                resolvedChannelGroups,
                query.getContext(),
                resolvedChannelGroups.size()
        );
    }

    private ResolvedChannelGroup resolveAnnotationData(
            QueryContext ctxt,
            ChannelGroup<?> channelGroup
    ) {
        ResolvedChannelGroup resolvedChannelGroup =
                ResolvedChannelGroup.create(channelGroup);

        if (contextHasAnnotation(ctxt, Annotation.REGIONS)) {
            resolvedChannelGroup.setRegions(
                    resolveRegionChannelGroups(channelGroup)
            );
        }

        return resolvedChannelGroup;
    }

    private Iterable<ChannelGroup<?>> resolveRegionChannelGroups(ChannelGroup entity) {

        try {
            if (entity instanceof Platform){
                Platform platform = (Platform) entity;
                Iterable<Id> regionIds = Iterables.transform(
                        platform.getRegions(),
                        new Function<ChannelGroupRef, Id>() {

                            @Override
                            public Id apply(ChannelGroupRef input) {
                                return input.getId();
                            }
                        }
                );

                return Futures.get(
                        Futures.transform(
                                resolver.resolveIds(regionIds),
                                (Resolved<ChannelGroup<?>> input) -> {
                                    return input.getResources();
                                }
                        ), 1, TimeUnit.MINUTES, IOException.class
                );
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return null;
    }

    private boolean contextHasAnnotation(QueryContext ctxt, Annotation annotation) {
        return !Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
            &&
            Splitter.on(',')
                    .splitToList(
                            ctxt.getRequest().getParameter("annotations")
                    ).contains(annotation.toKey());
    }
}
