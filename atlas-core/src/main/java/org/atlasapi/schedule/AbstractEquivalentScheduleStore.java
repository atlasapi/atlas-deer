package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;

/**
 * Base implementation of an {@link EquivalentScheduleStore} which resolves the necessary {@link Content}
 *  for {@link ScheduleUpdate}s and {@link EquivalenceUpdate}s.
 */
public abstract class AbstractEquivalentScheduleStore implements EquivalentScheduleStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractEquivalentScheduleStore.class);
    
    private final EquivalenceGraphStore graphStore;
    private final ContentResolver contentStore;
    
    //TODO make this more flexible.
    private final FlexibleBroadcastMatcher broadcastMatcher
        = new FlexibleBroadcastMatcher(Duration.standardMinutes(10));

    public AbstractEquivalentScheduleStore(EquivalenceGraphStore graphStore, ContentResolver contentStore) {
        this.graphStore = checkNotNull(graphStore);
        this.contentStore = checkNotNull(contentStore);
    }
    
    @Override
    public final void updateSchedule(ScheduleUpdate update) throws WriteException {

        writeSchedule(update, contentFor(update.getSchedule()));
    }



    protected abstract void writeSchedule(ScheduleUpdate update, Map<ScheduleRef.Entry, EquivalentScheduleEntry> content)
            throws WriteException;

    private Map<ScheduleRef.Entry, EquivalentScheduleEntry> contentFor(ScheduleRef schedule) throws WriteException {
        final List<Id> itemIds = Lists.transform(schedule.getScheduleEntries(), 
            new Function<ScheduleRef.Entry, Id>(){
                @Override
                public Id apply(ScheduleRef.Entry input) {
                    return input.getItem();
                }
            }
        );
        OptionalMap<Id, EquivalenceGraph> graphs = graphsFor(itemIds);
        Map<Id, Item> content = itemsFor(graphs, itemIds);
        return join(schedule.getScheduleEntries(), graphs, content);
    }

    private ImmutableMap<ScheduleRef.Entry, EquivalentScheduleEntry> join(List<ScheduleRef.Entry> entries, 
            OptionalMap<Id, EquivalenceGraph> graphs, Map<Id, Item> allItems) {
        Function<Id, Item> toItems = Functions.forMap(allItems, null);
        ImmutableMap.Builder<ScheduleRef.Entry, EquivalentScheduleEntry> entryContent = ImmutableMap.builder();
        for (ScheduleRef.Entry entry : entries) {
            Id itemId = entry.getItem();
            Item item = toItems.apply(itemId);
            if (item == null) {
                log.warn("No item for entry " + entry);
                continue;
            }
            item = item.copy();
            Broadcast broadcast = findBroadcast(item, entry);
            if (broadcast == null) {
                log.warn("No broadcast for entry " + entry);
                continue;
            }
            item.setBroadcasts(ImmutableSet.of(broadcast));
            Optional<EquivalenceGraph> possibleGraph = graphs.get(itemId);
            EquivalenceGraph graph = possibleGraph.isPresent() ? possibleGraph.get()
                                                               : EquivalenceGraph.valueOf(item.toRef());
            Equivalent<Item> equivItems = new Equivalent<Item>(graph, 
                    equivItems(item, broadcast, graphItems(graph, toItems)));
            entryContent.put(entry, new EquivalentScheduleEntry(broadcast, equivItems));
        }
        return entryContent.build();
    }

    private ImmutableSet<Item> equivItems(Item item, Broadcast broadcast, Iterable<Item> graphItems) {
        ImmutableSet<Item> items = ImmutableSet.<Item>builder()
            .add(item)
            .addAll(filterSources(itemsBySource(graphItems), broadcast, item.getSource()))
            .build();
        return items;
    }
    
    private Broadcast findBroadcast(Item broadcastItem, ScheduleRef.Entry entry) {
        BroadcastRef ref = entry.getBroadcast();
        for (Broadcast broadcast : broadcastItem.getBroadcasts()) {
            if (broadcast.getSourceId().equals(ref .getSourceId())
                || broadcast.getChannelId().equals(ref.getChannelId())
                && ref.getTransmissionInterval().equals(broadcast.getTransmissionInterval())) {
                return broadcast;
            }
        }
        return null;
    }

    private Iterable<Item> graphItems(EquivalenceGraph graph, Function<Id, Item> toContent) {
        return Iterables.filter(Iterables.transform(graph.getEquivalenceSet(), toContent), Predicates.notNull());
    }

    private Map<Id, Item> itemsFor(OptionalMap<Id, EquivalenceGraph> graphs, List<Id> itemIds)
            throws WriteException {
        Set<Id> graphIds = idsFrom(graphs.values(), itemIds);
        return get(contentStore.resolveIds(graphIds)).getResources()
                .filter(Item.class).uniqueIndex(Identifiables.toId());
    }

    private Set<Id> idsFrom(Collection<Optional<EquivalenceGraph>> values, List<Id> itemIds) {
        return ImmutableSet.<Id>builder()
                .addAll(itemIds) //include to be safe, they may not have graphs (yet).
                .addAll(graphIds(values))
                .build();
    }

    private Iterable<Id> graphIds(Collection<Optional<EquivalenceGraph>> values) {
        return Iterables.concat(Iterables.transform(Optional.presentInstances(values), 
            new Function<EquivalenceGraph, Iterable<Id>>(){
                @Override
                public Iterable<Id> apply(EquivalenceGraph input) {
                    return input.getEquivalenceSet();
                }
            }));
    }

    private OptionalMap<Id, EquivalenceGraph> graphsFor(final List<Id> itemIds) throws WriteException {
        return get(graphStore.resolveIds(itemIds));
    }

    private <T> T get(ListenableFuture<T> future) throws WriteException {
        return Futures.get(future, 1, TimeUnit.MINUTES, WriteException.class);
    }

    @Override
    public final void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException {
        for (EquivalenceGraph graph : update.getAllGraphs()) {
            Resolved<Content> graphContent = get(contentStore.resolveIds(graph.getEquivalenceSet()));
            for (Content elem : graphContent.getResources()) {
                if (elem instanceof Item) {
                    Item item = (Item) elem;
                    Publisher src = item.getSource();
                    for (Broadcast bcast : Iterables.filter(item.getBroadcasts(),Broadcast.ACTIVELY_PUBLISHED)) {
                        Item copy = item.copy();
                        copy.setBroadcasts(ImmutableSet.of(bcast));
                        updateEquivalentContent(src, bcast, graph, equivItems(copy, bcast,
                                graphContent.getResources().filter(Item.class)));
                    }
                }
            }
        }
    }

    private ImmutableMap<Publisher, Collection<Item>> itemsBySource(Iterable<Item> graphContent) {
        return Multimaps.index(graphContent, Sourceds.toPublisher()).asMap();
    }

    private ImmutableSet<Item> filterSources(Map<Publisher, Collection<Item>> contentBySource, Broadcast subjBcast, Publisher src) {
        ImmutableSet.Builder<Item> selected = ImmutableSet.builder();
        for (Map.Entry<Publisher, Collection<Item>> sourceContent : contentBySource.entrySet()) {
            if (sourceContent.getKey().equals(src)) {
                continue;
            }
            Item bestMatch = bestMatch(sourceContent.getValue(), subjBcast);
            if (bestMatch == null) {
                selected.addAll(matchingOrEmptyBroadcasts(subjBcast, sourceContent.getValue()));
            } else {
                bestMatch = bestMatch.copy();
                bestMatch.setBroadcasts(matchingOrEmpty(subjBcast, bestMatch.getBroadcasts()));
                selected.add(bestMatch);
            }
        }
        return selected.build();
    }

    private Set<Broadcast> matchingOrEmpty(Broadcast subjBcast, Set<Broadcast> broadcasts) {
        for (Broadcast broadcast : Iterables.filter(broadcasts, Broadcast.ACTIVELY_PUBLISHED)) {
            if (broadcastMatcher.matches(subjBcast, broadcast)) {
                return ImmutableSet.of(broadcast);
            }
        }
        return ImmutableSet.of();
    }

    private Iterable<? extends Item> matchingOrEmptyBroadcasts(final Broadcast subjBroadcast, Collection<Item> value) {
        return Iterables.transform(value, new Function<Item, Item>(){
            @Override
            public Item apply(Item input) {
                Item copy = input.copy();
                copy.setBroadcasts(matchingOrEmpty(subjBroadcast, copy.getBroadcasts()));
                return copy;
            }
        });
    }

    private Item bestMatch(Collection<Item> sourceContent, Broadcast subjBcast) {
        for (Item item : sourceContent) {
            if (broadcastMatch(item, subjBcast)) {
                return item;
            }
        }
        return null;
    }

    private boolean broadcastMatch(Item item, Broadcast subjBcast) {
        for (Broadcast broadcast : Iterables.filter(item.getBroadcasts(),Broadcast.ACTIVELY_PUBLISHED)) {
            if (broadcastMatcher.matches(subjBcast, broadcast)) {
                return true;
            }
        }
        return false;
    }

    protected abstract void updateEquivalentContent(Publisher publisher, Broadcast bcast,
            EquivalenceGraph graph, ImmutableSet<Item> content) throws WriteException;

}
