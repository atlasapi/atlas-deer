package org.atlasapi.query.v4.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationAccessRole;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.EquivalentChannelSchedule;
import org.atlasapi.schedule.EquivalentSchedule;
import org.atlasapi.schedule.EquivalentScheduleEntry;
import org.atlasapi.schedule.EquivalentScheduleResolver;
import org.atlasapi.schedule.FlexibleBroadcastMatcher;
import org.atlasapi.schedule.Schedule;

import com.metabroadcast.common.stream.MoreCollectors;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EquivalentScheduleQueryExecutor
        implements ScheduleQueryExecutor {

    private static final long QUERY_TIMEOUT = 60000;

    private ChannelResolver channelResolver;
    private EquivalentScheduleResolver scheduleResolver;
    private ApplicationEquivalentsMerger<Content> equivalentsMerger;
    private FlexibleBroadcastMatcher broadcastMatcher;
    private ScheduleMerger scheduleMerger = new ScheduleMergerImpl();

    public EquivalentScheduleQueryExecutor(
            ChannelResolver channelResolver,
            EquivalentScheduleResolver scheduleResolver,
            ApplicationEquivalentsMerger<Content> equivalentsMerger,
            FlexibleBroadcastMatcher broadcastMatcher
    ) {
        this.channelResolver = checkNotNull(channelResolver);
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.equivalentsMerger = checkNotNull(equivalentsMerger);
        this.broadcastMatcher = checkNotNull(broadcastMatcher);
    }

    @Override
    public QueryResult<ChannelSchedule> execute(ScheduleQuery query)
            throws QueryExecutionException {


        Iterable<Channel> channels = resolveChannels(query);

        ImmutableSet<Publisher> selectedSources = selectedSources(query);

        Ordering<ChannelSchedule> ordering = getQueryIdOrdering(query);

        ImmutableList<ChannelSchedule> orderedChannelSchedules;
        ImmutableList<ChannelSchedule> orderedOverrideSchedules = ImmutableList.of();

        orderedChannelSchedules = ImmutableList.copyOf(
                ordering.sortedCopy(
                        getChannelSchedules(channels, query, selectedSources)
                )
        );

        if(query.getOverride().isPresent()) {
            orderedOverrideSchedules = ImmutableList.copyOf(
                    ordering.sortedCopy(
                            getChannelSchedules(channels, query, selectedSources)
                    )
            );
        }

        if (query.isMultiChannel()) {
            List<ChannelSchedule> channelSchedules;

            if (query.getOverride().isPresent()) {

                if (orderedChannelSchedules.size() != orderedOverrideSchedules.size()) {
                    throw new IllegalStateException(
                            String.format("Original schedule should have same number "
                                    + "of items as override: %d vs. %d, %s | %s",
                                    orderedChannelSchedules.size(), orderedOverrideSchedules.size(),
                                    query.getChannelIds(), query.getOverride()
                            ));
                }

                channelSchedules = StreamUtils
                        .zip(orderedChannelSchedules.stream(), orderedOverrideSchedules.stream(),
                                scheduleMerger::merge)
                        .collect(MoreCollectors.toImmutableList());
            } else {
                channelSchedules = orderedChannelSchedules;
            }
            return QueryResult.listResult(
                    channelSchedules,
                    query.getContext(),
                    channelSchedules.size()
            );
        } else {
            ChannelSchedule originalSchedule =
                    Iterables.getOnlyElement(orderedChannelSchedules);
            if (!orderedOverrideSchedules.isEmpty()) {
                ChannelSchedule override = Iterables.getOnlyElement(orderedOverrideSchedules);
                return QueryResult.singleResult(
                        scheduleMerger.merge(originalSchedule, override),
                        query.getContext()
                );
            } else {
                return QueryResult.singleResult(originalSchedule, query.getContext());
            }
        }
    }

    private ImmutableSet<Publisher> selectedSources(ScheduleQuery query) {
        if (query.getContext().getApplication().getConfiguration().isPrecedenceEnabled()) {
            return query.getContext().getApplication().getConfiguration().getEnabledReadSources();
        }
        return ImmutableSet.of(query.getSource());
    }

    private Iterable<Channel> resolveChannels(ScheduleQuery query) throws QueryExecutionException {
        Iterable<Id> channelIds = query.isMultiChannel()
                                  ? query.getChannelIds()
                                  : ImmutableSet.of(query.getChannelId());

        Resolved<Channel> resolvedChannels = Futures.get(
                channelResolver.resolveIds(channelIds),
                1,
                TimeUnit.MINUTES,
                QueryExecutionException.class
        );
        if (resolvedChannels.getResources().isEmpty()) {
            throw new NotFoundException(Iterables.getFirst(channelIds, null));
        }
        return resolvedChannels.getResources();
    }

    private List<ChannelSchedule> channelSchedules(ListenableFuture<EquivalentSchedule> schedule,
            ScheduleQuery query)
            throws ScheduleQueryExecutionException {

        return Futures.get(Futures.transform(schedule, toSchedule(query.getContext())),
                QUERY_TIMEOUT, MILLISECONDS, ScheduleQueryExecutionException.class
        ).channelSchedules();
    }

    private Function<EquivalentSchedule, Schedule> toSchedule(final QueryContext context) {

        return input -> {
            boolean hasNonMergedAnnotation =
                    context.getAnnotations().containsValue(Annotation.NON_MERGED);

            if (context.getApplication().getConfiguration().isPrecedenceEnabled() && !hasNonMergedAnnotation) {
                return mergeItemsInSchedule(input, context.getApplication());
            }
            return selectBroadcastItems(input);
        };
    }

    private Schedule mergeItemsInSchedule(EquivalentSchedule schedule,
            Application application) {
        ImmutableList.Builder<ChannelSchedule> channelSchedules = ImmutableList.builder();
        for (EquivalentChannelSchedule ecs : schedule.channelSchedules()) {
            channelSchedules.add(new ChannelSchedule(
                    ecs.getChannel(),
                    ecs.getInterval(),
                    mergeItems(ecs.getEntries(), application)
            ));
        }
        return new Schedule(channelSchedules.build(), schedule.interval());
    }

    private Iterable<ItemAndBroadcast> mergeItems(ImmutableList<EquivalentScheduleEntry> entries,
            Application application) {
        ImmutableList.Builder<ItemAndBroadcast> iabs = ImmutableList.builder();
        for (EquivalentScheduleEntry entry : entries) {
            List<Item> mergedItems = equivalentsMerger.merge(
                    Optional.<Id>absent(),
                    entry.getItems().getResources(),
                    application
            );

            Item item = Iterables.getOnlyElement(mergedItems);
            Broadcast broadcast = broadcastMatcher.findMatchingBroadcast(
                    entry.getBroadcast(),
                    item.getBroadcasts()
            ).or(entry.getBroadcast());

            iabs.add(new ItemAndBroadcast(item, broadcast));
        }
        return iabs.build();
    }

    private Schedule selectBroadcastItems(EquivalentSchedule schedule) {
        ImmutableList.Builder<ChannelSchedule> channelSchedules = ImmutableList.builder();
        for (EquivalentChannelSchedule ecs : schedule.channelSchedules()) {
            channelSchedules.add(new ChannelSchedule(
                    ecs.getChannel(),
                    ecs.getInterval(),
                    selectBroadcastItems(ecs.getEntries())
            ));
        }
        return new Schedule(channelSchedules.build(), schedule.interval());
    }

    private Iterable<ItemAndBroadcast> selectBroadcastItems(List<EquivalentScheduleEntry> entries) {
        ImmutableList.Builder<ItemAndBroadcast> iabs = ImmutableList.builder();
        for (EquivalentScheduleEntry entry : entries) {
            iabs.add(new ItemAndBroadcast(selectBroadcastItem(entry), entry.getBroadcast()));
        }
        return iabs.build();
    }

    private Item selectBroadcastItem(EquivalentScheduleEntry entry) {
        for (Item item : entry.getItems().getResources()) {
            if (item.getBroadcasts().contains(entry.getBroadcast())) {
                return item;
            }
        }
        throw new IllegalStateException("couldn't find broadcast item in " + entry);
    }

    private ImmutableList<ChannelSchedule> getChannelSchedules(
            Iterable<Channel> channels,
            ScheduleQuery query,
            ImmutableSet<Publisher> selectedSources
    ) throws ScheduleQueryExecutionException {

        List<Channel> ebsChannels = new ArrayList<>();
        List<Channel> defaultChannels = new ArrayList<>();

        if (query.getContext()
                .getApplication()
                .getAccessRoles()
                .hasRole(ApplicationAccessRole.PREFER_EBS_SCHEDULE.getRole())
                ) {

            channels.forEach(channel -> {
                if (channel.getAvailableFrom().contains(Publisher.BT_SPORT_EBS)) {
                    ebsChannels.add(channel);
                }
            });
        }

        defaultChannels.addAll(Lists.newArrayList(channels));

        ListenableFuture<EquivalentSchedule> ebsSchedule;
        Publisher overridableEbsPublisher = query.getOverride().isPresent() ?
                                            query.getOverride().get() :
                                            Publisher.BT_SPORT_EBS;

        ListenableFuture<EquivalentSchedule> defaultSchedule;
        Publisher overridableDefaultPublisher = query.getOverride().isPresent() ?
                                                query.getOverride().get() :
                                                query.getSource();

        if (query.getEnd().isPresent()) {
            ebsSchedule = scheduleResolver.resolveSchedules(
                    ebsChannels,
                    new Interval(query.getStart(), query.getEnd().get()),
                    overridableEbsPublisher,
                    selectedSources
            );

            defaultSchedule = scheduleResolver.resolveSchedules(
                    defaultChannels,
                    new Interval(query.getStart(), query.getEnd().get()),
                    overridableDefaultPublisher,
                    selectedSources
            );
        } else {
            ebsSchedule = scheduleResolver.resolveSchedules(
                    ebsChannels,
                    query.getStart(),
                    query.getCount().get(),
                    overridableEbsPublisher,
                    selectedSources
            );

            defaultSchedule = scheduleResolver.resolveSchedules(
                    defaultChannels,
                    query.getStart(),
                    query.getCount().get(),
                    overridableDefaultPublisher,
                    selectedSources
            );
        }

        Map<String, ChannelSchedule> scheduleMap = new HashMap<>();

        if (!ebsChannels.isEmpty()) {
            channelSchedules(ebsSchedule, query).forEach(channelSchedule ->
                scheduleMap.put(
                        channelSchedule.getChannel().getKey(),
                        channelSchedule.copyWithScheduleSource(overridableEbsPublisher)
                ));
        }
        if (!defaultChannels.isEmpty()) {
            channelSchedules(defaultSchedule, query).forEach(channelSchedule ->
                scheduleMap.putIfAbsent(
                        channelSchedule.getChannel().getKey(),
                        channelSchedule.copyWithScheduleSource(overridableDefaultPublisher)
                ));
        }

        return ImmutableList.copyOf(scheduleMap.values());
    }

    private Ordering<ChannelSchedule> getQueryIdOrdering(ScheduleQuery query) {

        ImmutableList.Builder<Id> idOrderingBuilder = ImmutableList.builder();

        if (query.isMultiChannel()) {
            query.getChannelIds().forEach(idOrderingBuilder::add);
        } else {
            idOrderingBuilder.add(query.getChannelId());
        }

        return Ordering.explicit(idOrderingBuilder.build())
                .onResultOf(channelSchedule -> channelSchedule.getChannel().getId());
    }

}
