package org.atlasapi.query.v4.schedule;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.EquivalentChannelSchedule;
import org.atlasapi.schedule.EquivalentSchedule;
import org.atlasapi.schedule.EquivalentScheduleEntry;
import org.atlasapi.schedule.EquivalentScheduleResolver;
import org.atlasapi.schedule.FlexibleBroadcastMatcher;
import org.atlasapi.schedule.Schedule;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
        ListenableFuture<EquivalentSchedule> schedule;
        ListenableFuture<EquivalentSchedule> overrideSchedule = null;
        if (query.getEnd().isPresent()) {
            schedule = scheduleResolver.resolveSchedules(
                    channels,
                    new Interval(query.getStart(), query.getEnd().get()),
                    query.getSource(),
                    selectedSources
            );

            if (query.getOverride().isPresent()) {
                overrideSchedule = scheduleResolver.resolveSchedules(
                        channels,
                        new Interval(query.getStart(), query.getEnd().get()),
                        query.getOverride().get(),
                        selectedSources
                );
            }
        } else {
            schedule = scheduleResolver.resolveSchedules(
                    channels,
                    query.getStart(),
                    query.getCount().get(),
                    query.getSource(),
                    selectedSources
            );

            if (query.getOverride().isPresent()) {
                overrideSchedule = scheduleResolver.resolveSchedules(
                        channels,
                        query.getStart(),
                        query.getCount().get(),
                        query.getOverride().get(),
                        selectedSources
                );
            }
        }

        if (query.isMultiChannel()) {
            List<ChannelSchedule> channelSchedules;

            if (query.getOverride().isPresent()) {
                List<ChannelSchedule> originalSchedules = channelSchedules(schedule, query);
                List<ChannelSchedule> overrideSchedules = channelSchedules(overrideSchedule, query);

                if (originalSchedules.size() != overrideSchedules.size()) {
                    throw new IllegalStateException(
                            String.format("Original schedule should have same number "
                                    + "of items as override: %d vs. %d, %s | %s",
                                    originalSchedules.size(), overrideSchedules.size(),
                                    query.getChannelIds(), query.getOverride()
                            ));
                }

                channelSchedules = StreamUtils
                        .zip(originalSchedules.stream(), overrideSchedules.stream(),
                                scheduleMerger::merge)
                        .collect(Collectors.toList());
            } else {
                channelSchedules = channelSchedules(schedule, query);
            }
            return QueryResult.listResult(
                    channelSchedules,
                    query.getContext(),
                    channelSchedules.size()
            );
        } else {
            ChannelSchedule originalSchedule =
                    Iterables.getOnlyElement(channelSchedules(schedule, query));
            if (overrideSchedule != null) {
                ChannelSchedule override = Iterables.getOnlyElement(channelSchedules(
                        overrideSchedule,
                        query
                ));
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
        if (query.getContext().getApplicationSources().isPrecedenceEnabled()) {
            return query.getContext().getApplicationSources().getEnabledReadSources();
        }
        return ImmutableSet.of(query.getSource());
    }

    private Iterable<Channel> resolveChannels(ScheduleQuery query) throws QueryExecutionException {
        Iterable<Id> channelIds;
        if (query.isMultiChannel()) {
            channelIds = query.getChannelIds();
        } else {
            channelIds = ImmutableSet.of(query.getChannelId());
        }
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
        return new Function<EquivalentSchedule, Schedule>() {

            @Override
            public Schedule apply(EquivalentSchedule input) {
                if (context.getApplicationSources().isPrecedenceEnabled()) {
                    return mergeItemsInSchedule(input, context.getApplicationSources());
                }
                return selectBroadcastItems(input);
            }

        };
    }

    private Schedule mergeItemsInSchedule(EquivalentSchedule schedule,
            ApplicationSources applicationSources) {
        ImmutableList.Builder<ChannelSchedule> channelSchedules = ImmutableList.builder();
        for (EquivalentChannelSchedule ecs : schedule.channelSchedules()) {
            channelSchedules.add(new ChannelSchedule(
                    ecs.getChannel(),
                    ecs.getInterval(),
                    mergeItems(ecs.getEntries(), applicationSources)
            ));
        }
        return new Schedule(channelSchedules.build(), schedule.interval());
    }

    private Iterable<ItemAndBroadcast> mergeItems(ImmutableList<EquivalentScheduleEntry> entries,
            ApplicationSources applicationSources) {
        ImmutableList.Builder<ItemAndBroadcast> iabs = ImmutableList.builder();
        for (EquivalentScheduleEntry entry : entries) {
            List<Item> mergedItems = equivalentsMerger.merge(
                    Optional.<Id>absent(),
                    entry.getItems().getResources(),
                    applicationSources
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

}
