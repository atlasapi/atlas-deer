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
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.locks.GroupLock;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractEquivalentScheduleStore implements EquivalentScheduleStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractEquivalentScheduleStore.class);

    private static final String METER_CALLED = "meter.called";
    private static final String METER_FAILURE = "meter.failure";

    protected final MetricRegistry metricRegistry;

    private final EquivalenceGraphStore graphStore;
    private final ContentResolver contentStore;

    private final FlexibleBroadcastMatcher broadcastMatcher
            = new FlexibleBroadcastMatcher(Duration.standardMinutes(10));

    private final GroupLock<String> lock;

    private final String updateSchedule;
    private final String updateEquivalences;


    public AbstractEquivalentScheduleStore(
            EquivalenceGraphStore graphStore,
            ContentResolver contentStore,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        this.graphStore = checkNotNull(graphStore);
        this.contentStore = checkNotNull(contentStore);

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

        // Fire all the requests to the content store in parallel
        ImmutableMap<EquivalenceGraph, ListenableFuture<Resolved<Content>>> graphsContent = graphs
                .stream()
                .distinct()
                .collect(MoreCollectors.toImmutableMap(
                        graph -> graph,
                        graph -> contentStore.resolveIds(graph.getEquivalenceSet())
                ));

        ImmutableList.Builder<ResultSetFuture> futureBuilder = ImmutableList.builder();

        // Create a future for each update we will have to do
        for (Map.Entry<EquivalenceGraph, ListenableFuture<Resolved<Content>>> entry :
                graphsContent.entrySet()) {
            EquivalenceGraph graph = entry.getKey();
            ImmutableList<Item> graphItems = get(entry.getValue())
                    .getResources()
                    .filter(Item.class)
                    .toList();

            metricRegistry.histogram(updateEquivalences + "graph.histogram.size")
                    .update(graphItems.size());

            for (Item item : graphItems) {
                ImmutableList<Broadcast> broadcasts = item.getBroadcasts()
                        .stream()
                        .filter(Broadcast::isActivelyPublished)
                        .collect(MoreCollectors.toImmutableList());

                for (Broadcast broadcast : broadcasts) {
                    Item copy = item.copy();
                    copy.setBroadcasts(ImmutableSet.of(broadcast));

                    metricRegistry.meter(
                            updateEquivalences + "source."
                                    + item.getSource().key().replace('.', '_') + "." + METER_CALLED
                    )
                            .mark();

                    metricRegistry.meter(
                            updateEquivalences + "broadcast." + getBroadcastMetricName(broadcast)
                                    + METER_CALLED
                    )
                            .mark();

                    futureBuilder.addAll(updateEquivalentContent(
                            item.getSource(),
                            broadcast,
                            graph,
                            equivItems(copy, broadcast, graphItems)
                    ));
                }
            }
        }

        ImmutableList<ResultSetFuture> futures = futureBuilder.build();

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
                    graph, equivItems(item, broadcast, graphItems(graph, allItems))
            );
            entryContent.put(entry, new EquivalentScheduleEntry(broadcast, equivItems));
        }
        return entryContent.build();
    }

    private ImmutableSet<Item> equivItems(Item item, Broadcast broadcast,
            Iterable<Item> graphItems) {
        return ImmutableSet.<Item>builder()
                .add(item)
                .addAll(filterSources(itemsBySource(graphItems), broadcast, item.getSource()))
                .build();
    }

    private java.util.Optional<Broadcast> findBroadcast(Item broadcastItem,
            ScheduleRef.Entry entry) {
        BroadcastRef ref = entry.getBroadcast();

        return broadcastItem.getBroadcasts()
                .stream()
                .filter(broadcast -> broadcast.getSourceId().equals(ref.getSourceId())
                        || broadcast.getChannelId().equals(ref.getChannelId())
                        && ref.getTransmissionInterval().equals(broadcast.getTransmissionInterval())
                )
                .findFirst();
    }

    private Iterable<Item> graphItems(EquivalenceGraph graph, Map<Id, Item> itemMap) {
        return graph.getEquivalenceSet()
                .stream()
                .map(itemMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
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

    private ImmutableMap<Publisher, Collection<Item>> itemsBySource(Iterable<Item> graphContent) {
        return Multimaps.index(graphContent, Sourceds.toPublisher()).asMap();
    }

    private ImmutableSet<Item> filterSources(Map<Publisher, Collection<Item>> contentBySource,
            Broadcast subjectBroadacast, Publisher src) {
        ImmutableSet.Builder<Item> selected = ImmutableSet.builder();
        for (Map.Entry<Publisher, Collection<Item>> sourceContent : contentBySource.entrySet()) {
            if (sourceContent.getKey().equals(src)) {
                continue;
            }

            java.util.Optional<Item> bestMatch = bestMatch(
                    sourceContent.getValue(), subjectBroadacast
            );

            if (bestMatch.isPresent()) {
                Item item = bestMatch.get().copy();
                item.setBroadcasts(matchingOrEmpty(subjectBroadacast, item.getBroadcasts()));
                selected.add(item);
            } else {
                selected.addAll(matchingOrEmptyBroadcasts(
                        subjectBroadacast, sourceContent.getValue()
                ));
            }
        }
        return selected.build();
    }

    private Set<Broadcast> matchingOrEmpty(Broadcast subjectBroadcast, Set<Broadcast> broadcasts) {
        return broadcasts.stream()
                .filter(Broadcast::isActivelyPublished)
                .filter(broadcast -> broadcastMatcher.matches(subjectBroadcast, broadcast))
                .findFirst()
                .map(ImmutableSet::of)
                .orElse(ImmutableSet.of());
    }

    private Iterable<? extends Item> matchingOrEmptyBroadcasts(Broadcast subjectBroadcast,
            Collection<Item> value) {
        return value.stream()
                .map(input -> {
                    Item copy = input.copy();
                    copy.setBroadcasts(matchingOrEmpty(subjectBroadcast, copy.getBroadcasts()));
                    return copy;
                })
                .collect(Collectors.toList());
    }

    private java.util.Optional<Item> bestMatch(Collection<Item> sourceContent,
            Broadcast subjectBroadcast) {
        return sourceContent.stream()
                .filter(item -> broadcastMatch(item, subjectBroadcast))
                .findFirst();
    }

    private boolean broadcastMatch(Item item, Broadcast subjectBroadcast) {
        return item.getBroadcasts()
                .stream()
                .filter(Broadcast::isActivelyPublished)
                .anyMatch(broadcast -> broadcastMatcher.matches(subjectBroadcast, broadcast));
    }

    private String getBroadcastMetricName(Broadcast broadcast) {
        if (broadcast.getTransmissionEndTime().isAfterNow()) {
            return "current.";
        }

        Duration broadcastAge = new Duration(
                broadcast.getTransmissionEndTime(),
                DateTime.now()
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
