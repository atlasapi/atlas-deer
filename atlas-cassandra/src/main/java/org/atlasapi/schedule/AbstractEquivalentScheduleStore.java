package org.atlasapi.schedule;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.locks.GroupLock;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractEquivalentScheduleStore implements EquivalentScheduleStore {

    private static final Logger log = LoggerFactory.getLogger(
            AbstractEquivalentScheduleStore.class
    );

    @VisibleForTesting
    static final Duration MAX_BROADCAST_AGE_TO_UPDATE = Duration.standardDays(14);

    private static final String METER_CALLED = "meter.called";
    private static final String METER_FAILURE = "meter.failure";

    protected final MetricRegistry metricRegistry;
    protected final Clock clock;

    private final EquivalenceGraphStore graphStore;
    protected final ContentResolver contentStore;

    private final FlexibleBroadcastMatcher broadcastMatcher
            = new FlexibleBroadcastMatcher(Duration.standardMinutes(10));

    private final GroupLock<String> lock;

    private final String updateSchedule;
    private final String updateEquivalences;

    public AbstractEquivalentScheduleStore(
            EquivalenceGraphStore graphStore,
            ContentResolver contentStore,
            MetricRegistry metricRegistry,
            String metricPrefix,
            Clock clock
    ) {
        this.graphStore = checkNotNull(graphStore);
        this.contentStore = checkNotNull(contentStore);
        this.clock = checkNotNull(clock);

        this.lock = GroupLock.natural(metricRegistry, metricPrefix);

        this.metricRegistry = metricRegistry;
        this.updateSchedule = metricPrefix + "updateSchedule.";
        this.updateEquivalences = metricPrefix + "updateEquivalences.";
    }

    @Override
    public final void updateSchedule(ScheduleUpdate update) throws WriteException {
        metricRegistry.meter(updateSchedule + METER_CALLED).mark();

        List<LocalDate> daysInSchedule = daysIn(update.getSchedule().getInterval());

        ImmutableList<LocalDate> staleBroadcastDays = update.getStaleBroadcasts()
                .stream()
                .map(broadcastRef -> broadcastRef.getTransmissionInterval()
                        .getStart()
                        .toLocalDate())
                .collect(MoreCollectors.toImmutableList());

        ImmutableSet<String> lockKeys = getLockKeys(
                update.getSchedule().getChannel(),
                update.getSource(),
                ImmutableSet.<LocalDate>builder()
                        .addAll(daysInSchedule)
                        .addAll(staleBroadcastDays)
                        .build()
        );

        try {
            lock.lock(lockKeys);
            writeSchedule(update, contentFor(update.getSchedule()));
        } catch (InterruptedException | WriteException | RuntimeException e) {
            metricRegistry.meter(updateSchedule + METER_FAILURE).mark();
            throw Throwables.propagate(e);
        } finally {
            lock.unlock(lockKeys);
        }
    }

    @Override
    public final void updateEquivalences(ImmutableSet<EquivalenceGraph> graphs)
            throws WriteException {
        metricRegistry.meter(updateEquivalences + METER_CALLED).mark();
        metricRegistry.histogram(updateEquivalences + "histogram.numberOfGraphs")
                .update(graphs.size());

        for (EquivalenceGraph graph : graphs) {
            updateEquivalences(graph);
        }
    }

    protected abstract void writeSchedule(
            ScheduleUpdate update,
            Map<ScheduleRef.Entry, EquivalentScheduleEntry> content
    ) throws WriteException;

    protected abstract ImmutableList<ResultSetFuture> updateEquivalentContent(
            Publisher publisher,
            Broadcast broadcast,
            EquivalenceGraph graph,
            ImmutableSet<Item> content
    );

    protected List<LocalDate> daysIn(Interval interval) {
        return StreamSupport.stream(new ScheduleIntervalDates(interval).spliterator(), false)
                .collect(Collectors.toList());
    }

    protected boolean shouldUpdateBroadcast(Broadcast broadcast) {
        return broadcast.getTransmissionEndTime()
                .isAfter(clock.now().minus(MAX_BROADCAST_AGE_TO_UPDATE));
    }

    private void updateEquivalences(EquivalenceGraph graph) throws WriteException {
        ImmutableList<Item> graphItems = get(
                contentStore.resolveIds(graph.getEquivalenceSet())
        )
                .getResources()
                .filter(Item.class)
                .toList();

        metricRegistry.histogram(updateEquivalences + "graph.histogram.size")
                .update(graphItems.size());

        ImmutableList<ResultSetFuture> futures = getUpdateEquivalencesFutures(graph, graphItems);

        metricRegistry.histogram(updateEquivalences + "histogram.parallelWrites")
                .update(futures.size());

        try {
            // Block until all futures are completed
            Futures.allAsList(futures).get();
        } catch (Exception e) {
            metricRegistry.meter(updateEquivalences + METER_FAILURE).mark();
            throw new WriteException(e);
        }
    }

    private ImmutableList<ResultSetFuture> getUpdateEquivalencesFutures(
            EquivalenceGraph graph,
            ImmutableList<Item> graphItems
    ) {
        ImmutableList.Builder<ResultSetFuture> futureBuilder = ImmutableList.builder();

        for (Item item : graphItems) {
            ImmutableList<Broadcast> broadcasts = item.getBroadcasts()
                    .stream()
                    .filter(Broadcast::isActivelyPublished)
                    .collect(MoreCollectors.toImmutableList());

            for (Broadcast broadcast : broadcasts) {
                metricRegistry.meter(
                        updateEquivalences + "source."
                                + item.getSource().key().replace('.', '_') + "." + METER_CALLED
                )
                        .mark();

                if (shouldUpdateBroadcast(broadcast)) {
                    metricRegistry.meter(
                            updateEquivalences + "broadcast." + getBroadcastMetricName(broadcast)
                                    + METER_CALLED
                    )
                            .mark();

                    futureBuilder.addAll(updateEquivalentContent(
                            item.getSource(),
                            broadcast,
                            graph,
                            updateItemsToKeepOnlyMatchingBroadcasts(
                                    broadcast,
                                    graphItems
                            )
                    ));
                } else {
                    metricRegistry.meter(updateEquivalences + "meter.nop").mark();
                }
            }
        }

        return futureBuilder.build();
    }

    private ImmutableSet<String> getLockKeys(
            Id channelId,
            Publisher source,
            Iterable<LocalDate> dates
    ) {
        return StreamSupport.stream(dates.spliterator(), false)
                .map(date -> getLockKey(channelId, source, date))
                .collect(MoreCollectors.toImmutableSet());
    }

    private String getLockKey(Id channelId, Publisher source, LocalDate date) {
        return channelId.longValue() + "|" + source.key() + "|" + date.toString();
    }

    private Map<ScheduleRef.Entry, EquivalentScheduleEntry> contentFor(ScheduleRef schedule)
            throws WriteException {
        List<Id> itemIds = Lists.transform(
                schedule.getScheduleEntries(), ScheduleRef.Entry::getItem
        );

        OptionalMap<Id, EquivalenceGraph> graphs = get(graphStore.resolveIds(itemIds));
        Map<Id, Item> content = itemsFor(graphs, itemIds);

        return join(schedule.getScheduleEntries(), graphs, content);
    }

    private ImmutableMap<ScheduleRef.Entry, EquivalentScheduleEntry> join(
            List<ScheduleRef.Entry> entries,
            OptionalMap<Id, EquivalenceGraph> graphs, Map<Id, Item> allItems
    ) {
        ImmutableMap.Builder<ScheduleRef.Entry, EquivalentScheduleEntry> entryContent =
                ImmutableMap.builder();

        for (ScheduleRef.Entry entry : entries) {
            Id itemId = entry.getItem();
            Item item = allItems.get(itemId);

            if (item == null) {
                log.warn("No item for entry " + entry);
                continue;
            }

            item = item.copy();

            java.util.Optional<Broadcast> broadcastOptional = findBroadcast(item, entry);
            if (!broadcastOptional.isPresent()) {
                log.warn("No broadcast for entry " + entry);
                continue;
            }
            Broadcast broadcast = broadcastOptional.get();

            item.setBroadcasts(ImmutableSet.of(broadcast));

            Optional<EquivalenceGraph> possibleGraph = graphs.get(itemId);
            EquivalenceGraph graph = possibleGraph.isPresent()
                                     ? possibleGraph.get()
                                     : EquivalenceGraph.valueOf(item.toRef());

            Equivalent<Item> equivItems = new Equivalent<>(
                    graph,
                    updateItemsToKeepOnlyMatchingBroadcasts(
                            broadcast,
                            graphItems(graph, allItems)
                    )
            );
            entryContent.put(
                    entry,
                    EquivalentScheduleEntry.create(broadcast, item.getId(), equivItems)
            );
        }
        return entryContent.build();
    }

    private java.util.Optional<Broadcast> findBroadcast(
            Item broadcastItem,
            ScheduleRef.Entry entry
    ) {
        BroadcastRef ref = entry.getBroadcast();

        return broadcastItem.getBroadcasts()
                .stream()
                .filter(Broadcast::isActivelyPublished)
                .filter(broadcast -> broadcast.getSourceId().equals(ref.getSourceId())
                        || broadcast.getChannelId().equals(ref.getChannelId())
                        && ref.getTransmissionInterval().equals(broadcast.getTransmissionInterval())
                )
                .findFirst();
    }

    private ImmutableList<Item> graphItems(EquivalenceGraph graph, Map<Id, Item> itemMap) {
        return graph.getEquivalenceSet()
                .stream()
                .map(itemMap::get)
                .filter(Objects::nonNull)
                .collect(MoreCollectors.toImmutableList());
    }

    private Map<Id, Item> itemsFor(OptionalMap<Id, EquivalenceGraph> graphs, List<Id> itemIds)
            throws WriteException {
        Set<Id> graphIds = idsFrom(graphs.values(), itemIds);
        return get(contentStore.resolveIds(graphIds))
                .getResources()
                .filter(Item.class)
                .uniqueIndex(Identifiables.toId());
    }

    private Set<Id> idsFrom(Collection<Optional<EquivalenceGraph>> values, List<Id> itemIds) {
        return ImmutableSet.<Id>builder()
                .addAll(itemIds) //include to be safe, they may not have graphs (yet).
                .addAll(graphIds(values))
                .build();
    }

    private Iterable<Id> graphIds(Collection<Optional<EquivalenceGraph>> values) {
        return StreamSupport.stream(Optional.presentInstances(values).spliterator(), false)
                .flatMap(graph -> graph.getEquivalenceSet().stream())
                .collect(MoreCollectors.toImmutableList());
    }

    private <T> T get(ListenableFuture<T> future) throws WriteException {
        return Futures.get(future, 1, TimeUnit.MINUTES, WriteException.class);
    }

    private ImmutableSet<Item> updateItemsToKeepOnlyMatchingBroadcasts(
            Broadcast subjectBroadcast,
            ImmutableList<Item> value
    ) {
        return value.stream()
                .map(input -> {
                    Item copy = input.copy();
                    copy.setBroadcasts(
                            findMatchingBroadcast(subjectBroadcast, copy.getBroadcasts())
                                    .map(ImmutableSet::of)
                                    .orElse(ImmutableSet.of())
                    );
                    return copy;
                })
                .collect(MoreCollectors.toImmutableSet());
    }

    private java.util.Optional<Broadcast> findMatchingBroadcast(
            Broadcast subjectBroadcast,
            Set<Broadcast> broadcasts
    ) {
        return broadcasts.stream()
                .filter(Broadcast::isActivelyPublished)
                .filter(broadcast -> broadcastMatcher.matches(subjectBroadcast, broadcast))
                .findFirst();
    }

    private String getBroadcastMetricName(Broadcast broadcast) {
        if (broadcast.getTransmissionEndTime().isAfterNow()) {
            return "current.";
        }

        Duration broadcastAge = new Duration(
                broadcast.getTransmissionEndTime(),
                clock.now()
        );

        if (broadcastAge.isShorterThan(Duration.standardDays(7))) {
            return "withinLastWeek.";
        }
        if (broadcastAge.isShorterThan(Duration.standardDays(30))) {
            return "withinLastMonth.";
        }
        if (broadcastAge.isShorterThan(Duration.standardDays(365))) {
            return "withinLastYear.";
        }
        return "olderThanOneYear.";
    }
}
