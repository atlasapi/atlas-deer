package org.atlasapi.equivalence;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.StoreException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.GroupLock;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

public abstract class AbstractEquivalenceGraphStore implements EquivalenceGraphStore {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEquivalenceGraphStore.class);
    private static final int TIMEOUT = 1;
    private static final TimeUnit TIMEOUT_UNITS = TimeUnit.MINUTES;
    
    private static final Pattern MISSING_ID_FROM_MAP = Pattern.compile("Key '([0-9]*)' not present in map");

    private final MessageSender<EquivalenceGraphUpdateMessage> messageSender;
    
    public AbstractEquivalenceGraphStore(MessageSender<EquivalenceGraphUpdateMessage> messageSender) {
        this.messageSender = checkNotNull(messageSender);
    }
    
    @Override
    public final Optional<EquivalenceGraphUpdate> updateEquivalences(ResourceRef subject,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) throws WriteException {

        ImmutableSet<Id> newAdjacents
            = ImmutableSet.copyOf(Iterables.transform(assertedAdjacents, Identifiables.toId()));
        Set<Id> subjectAndAdjacents = MoreSets.add(newAdjacents, subject.getId());
        Set<Id> transitiveSetsIds = null;
        try {
            LOG.debug("Thread {} is trying to enter synchronized block to lock graph IDs", Thread.currentThread().getName());
            synchronized (lock()) {
                LOG.debug("Thread {} has entered synchronized block to lock graph IDs {}", Thread.currentThread().getName(), subjectAndAdjacents);
                while((transitiveSetsIds = tryLockAllIds(subjectAndAdjacents)) == null) {
                    lock().unlock(subjectAndAdjacents);
                    lock().wait(5000);
                    LOG.debug("Thread {} attempting to lock IDs {}", Thread.currentThread().getName(), subjectAndAdjacents);
                }
            }
            LOG.debug("Thread {} has left synchronized block, having locked graph IDs", Thread.currentThread().getName());

            Optional<EquivalenceGraphUpdate> updated
                = updateGraphs(subject, ImmutableSet.copyOf(assertedAdjacents), sources);
            if (updated.isPresent()) {
                sendUpdateMessage(subject, updated);
            }
            return updated;
            
        } catch(InterruptedException e) {
            log().error(String.format("%s: %s", subject, newAdjacents), e);
            return Optional.absent();
        } catch (StoreException e) {
            Throwables.propagateIfPossible(e, WriteException.class);
            throw new WriteException(e);
        } catch (CorruptEquivalenceGraphIndexException e) {
            cleanGraphAndIndex(e.getSubjectId());
            /*
             TODO: the proper fix for this is making the lock reentrant, however that also breaks
                   other places where the lock is used. This is a workaround to prevent deadlocks
                   when there's an exception.
             */
            unlock(subjectAndAdjacents, transitiveSetsIds);
            return updateEquivalences(subject, assertedAdjacents, sources);
        } finally {
            unlock(subjectAndAdjacents, transitiveSetsIds);
        }
    }

    private void unlock(Set<Id> subjectAndAdjacents, Set<Id> transitiveSetsIds) {
        LOG.debug("Thread {} waiting for lock to unlock {} and {}",
                Thread.currentThread().getName(), subjectAndAdjacents, transitiveSetsIds);

        synchronized (lock()) {
            LOG.debug("Thread {} unlocking IDs {}",
                    Thread.currentThread().getName(), subjectAndAdjacents);

            lock().unlock(subjectAndAdjacents);

            if (transitiveSetsIds != null) {
                LOG.debug("Thread {} unlocking transitive IDs {}",
                        Thread.currentThread().getName(), transitiveSetsIds);

                lock().unlock(transitiveSetsIds);
            }
            LOG.debug("Thread {} performing notifyAll", Thread.currentThread().getName(), transitiveSetsIds);

            lock().notifyAll();
        }
    }

    protected abstract void doStore(ImmutableSet<EquivalenceGraph> graphs);

    protected abstract void cleanGraphAndIndex(Id subjectId);

    protected abstract GroupLock<Id> lock();

    protected abstract Logger log();

    private void sendUpdateMessage(ResourceRef subject, Optional<EquivalenceGraphUpdate> updated)  {
        try {
            messageSender.sendMessage(new EquivalenceGraphUpdateMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(DateTime.now(DateTimeZones.UTC)),
                updated.get()
            ));
        } catch (MessagingException e) {
            log().warn("messaging failed for equivalence update of " + subject, e);
        }
    }

    private Set<Id> tryLockAllIds(Set<Id> adjacentsIds) throws InterruptedException, StoreException {
        if (!lock().tryLock(adjacentsIds)) {
            LOG.debug("Thread {} failed to lock adjacents {}", Thread.currentThread().getName(), adjacentsIds);
            return null;
        }
        Iterable<Id> transitiveIds = transitiveIdsToLock(adjacentsIds);
        Set<Id> allIds = ImmutableSet.copyOf(Iterables.concat(transitiveIds, adjacentsIds));

        Iterable<Id> idsToLock = Iterables.filter(allIds, not(in(adjacentsIds)));
        boolean locked = lock().tryLock(ImmutableSet.copyOf(idsToLock));
        LOG.debug("Thread {} Lock attempt success status {} locking transitive set {}", Thread.currentThread().getName(), locked, idsToLock);
        return locked ? allIds : null;
    }

    private Iterable<Id> transitiveIdsToLock(Set<Id> adjacentsIds) throws StoreException {
        return Iterables.concat(Iterables.transform(get(resolveIds(adjacentsIds)).values(),
                input -> input.isPresent()
                         ? input.get().getAdjacencyList().keySet()
                         : ImmutableSet.<Id>of()
        ));
    }

    private Optional<EquivalenceGraphUpdate> updateGraphs(ResourceRef subject, 
            ImmutableSet<ResourceRef> assertedAdjacents, Set<Publisher> sources) throws StoreException, CorruptEquivalenceGraphIndexException {

        Optional<EquivalenceGraph> optionalSubjGraph = existingGraph(subject);
        EquivalenceGraph subjGraph = optionalSubjGraph.or(EquivalenceGraph.valueOf(subject));
        Adjacents subAdjs = subjGraph.getAdjacents(subject);
        
        if(subAdjs == null) {
            throw new CorruptEquivalenceGraphIndexException(subject.getId());
        }

        OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraphs = existingGraphMaps(
                assertedAdjacents);

        if(optionalSubjGraph.isPresent() &&
                checkForOrphanedContent(subjGraph, adjacentsExistingGraphs, assertedAdjacents)) {
            LOG.warn("Found orphaned content in graph {}. Rewriting graph in store and retrying",
                    subjGraph.getId().longValue());
            store(ImmutableSet.of(subjGraph));
            adjacentsExistingGraphs = existingGraphMaps(assertedAdjacents);
        }

        Map<ResourceRef, EquivalenceGraph> assertedAdjacentGraphs = resolveRefs(assertedAdjacents,
                adjacentsExistingGraphs);

        Map<Id, Adjacents> updatedAdjacents = updateAdjacencies(subject,
                subjGraph.getAdjacencyList().values(), assertedAdjacentGraphs, sources);
        
        EquivalenceGraphUpdate update =
                computeUpdate(subject, assertedAdjacentGraphs, updatedAdjacents);

        if(changeInAdjacents(subAdjs, assertedAdjacents, sources)) {
            store(update.getAllGraphs());
        }
        else {
            log().debug("{}: no change in neighbours: {}", subject, assertedAdjacents);
        }

        // We are always returning a graph update so that a graph change message will be
        // generated and downstream consumers (equivalent content index) will get a chance
        // to get up to date if they happen to fall out of sync with the graph store
        return Optional.of(update);
    }

    private EquivalenceGraphUpdate computeUpdate(ResourceRef subject,
            Map<ResourceRef, EquivalenceGraph> assertedAdjacentGraphs, Map<Id, Adjacents> updatedAdjacents) throws CorruptEquivalenceGraphIndexException {
        Map<Id, EquivalenceGraph> updatedGraphs = computeUpdatedGraphs(updatedAdjacents);
        EquivalenceGraph updatedGraph = graphFor(subject, updatedGraphs);
        return new EquivalenceGraphUpdate(updatedGraph,
            Collections2.filter(updatedGraphs.values(), Predicates.not(Predicates.equalTo(updatedGraph))),
            Iterables.filter(Iterables.transform(assertedAdjacentGraphs.values(), Identifiables.toId()),
                    Predicates.not(Predicates.in(updatedGraphs.keySet())))
        );
    }

    private EquivalenceGraph graphFor(ResourceRef subject, Map<Id, EquivalenceGraph> updatedGraphs) {
        for (EquivalenceGraph graph : updatedGraphs.values()) {
            if (graph.getEquivalenceSet().contains(subject.getId())) {
                return graph;
            }
        }
        throw new IllegalStateException("Couldn't find updated graph for " + subject);
    }

    private Map<Id, EquivalenceGraph> computeUpdatedGraphs(Map<Id, Adjacents> updatedAdjacents) throws CorruptEquivalenceGraphIndexException {
        Function<Identifiable, Adjacents> toAdjs =
                Functions.compose(Functions.forMap(updatedAdjacents), Identifiables.toId());
        Set<Id> seen = Sets.newHashSetWithExpectedSize(updatedAdjacents.size());

        Map<Id, EquivalenceGraph> updated = Maps.newHashMap();
        for (Adjacents adj : updatedAdjacents.values()) {
            if (!seen.contains(adj.getId())) {
                EquivalenceGraph graph = EquivalenceGraph.valueOf(transitiveSet(adj, toAdjs));
                updated.put(graph.getId(), graph);
                seen.addAll(graph.getEquivalenceSet());
            }
        }
        return updated;
    }

    private Set<Adjacents> transitiveSet(Adjacents adj, Function<Identifiable, Adjacents> toAdjs) throws CorruptEquivalenceGraphIndexException {
        Set<Adjacents> set = Sets.newHashSet();
        Predicate<Adjacents> notSeen = Predicates.not(Predicates.in(set));

        Queue<Adjacents> work = Lists.newLinkedList();
        work.add(adj);
        while(!work.isEmpty()) {
            Adjacents curr = work.poll();
            set.add(curr);
            try {
                work.addAll(Collections2.filter(Collections2.transform(curr.getAdjacent(), toAdjs),notSeen));
            } catch (IllegalArgumentException iae) {
                Matcher matcher = MISSING_ID_FROM_MAP.matcher(iae.getMessage());
                if (matcher.find()) {
                    throw new CorruptEquivalenceGraphIndexException(Id.valueOf(matcher.group(1)));
                }
                throw iae;
            }
        }
        return set;
    }

    private Map<Id, Adjacents> updateAdjacencies(ResourceRef subject,
            Iterable<Adjacents> subjAdjacencies, Map<ResourceRef, EquivalenceGraph> adjacentGraphs,
            Set<Publisher> sources) throws StoreException {
        Map<Id, Adjacents> updated = Maps.newHashMap();

        ImmutableSet<Adjacents> allAdjacents = currentTransitiveAdjacents(adjacentGraphs)
                .addAll(subjAdjacencies).build();
        for (Adjacents adj : allAdjacents) {
            if (updated.containsKey(adj.getId())) {
                updated.put(adj.getId(), updateAdjacents(updated.get(adj.getId()), subject, adjacentGraphs.keySet(), sources));
            } else {
                updated.put(adj.getId(), updateAdjacents(adj, subject, adjacentGraphs.keySet(), sources));
            }
        }
        return ImmutableMap.copyOf(updated);
    }

    private Adjacents updateAdjacents(Adjacents adj, ResourceRef subject,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        Adjacents result = adj;
        if (subject.equals(adj.getRef())) {
            result = updateSubjectAdjacents(adj, assertedAdjacents, sources);
        } else if (sources.contains(adj.getSource())) {
            if (assertedAdjacents.contains(adj.getRef())) {
                result = adj.copyWithAfferent(subject);
            } else if (adj.hasAfferentAdjacent(subject)) {
                result = adj.copyWithoutAfferent(subject);
            }
        }
        return result;
    }

    private Adjacents updateSubjectAdjacents(Adjacents subj,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        ImmutableSet.Builder<ResourceRef> updatedEfferents = ImmutableSet.<ResourceRef>builder()
            .add(subj.getRef())
            .addAll(assertedAdjacents)
            .addAll(Sets.filter(subj.getEfferent(), Predicates.not(Sourceds.sourceFilter(sources))));
        return subj.copyWithEfferents(updatedEfferents.build());
    }

    private ImmutableSet.Builder<Adjacents> currentTransitiveAdjacents(Map<ResourceRef, EquivalenceGraph> resolved)
            throws StoreException {
        ImmutableSet.Builder<Adjacents> result = ImmutableSet.<Adjacents>builder();
        for (EquivalenceGraph graph : resolved.values()) {
            result.addAll(graph.getAdjacencyList().values());
        }
        return result;
    }

    // This is checking for situations where a piece of content has been orphaned, i.e. it exists
    // in the subject graph, but when trying to resolve its own graph that graph does not exist
    // This is intended to fix orphaned items in the data source that were the result of an
    // unknown bug
    private boolean checkForOrphanedContent(EquivalenceGraph subjGraph,
            OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraph,
            Set<ResourceRef> assertedAdjacents) {
        return assertedAdjacents.stream()
                .anyMatch(adjacent -> contentIsOrphaned(subjGraph, adjacentsExistingGraph,
                        adjacent.getId()));
    }

    private boolean contentIsOrphaned(EquivalenceGraph subjGraph,
            OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraphs, Id assertedAdjacentId) {
        return subjGraph.getEquivalenceSet().contains(assertedAdjacentId)
                && !adjacentsExistingGraphs.get(assertedAdjacentId).isPresent();
    }

    private Map<ResourceRef, EquivalenceGraph> resolveRefs(Set<ResourceRef> adjacents,
            OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraph)
            throws WriteException {
        Map<ResourceRef, EquivalenceGraph> graphs = Maps.newHashMapWithExpectedSize(adjacents.size());
        for (ResourceRef adj : adjacents) {
            graphs.put(adj, adjacentsExistingGraph.get(adj.getId())
                    .or(EquivalenceGraph.valueOf(adj)));
        }
        return graphs;
    }

    private boolean changeInAdjacents(Adjacents subjAdjs,
            ImmutableSet<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        Set<ResourceRef> currentNeighbours
            = Sets.filter(subjAdjs.getEfferent(), Sourceds.sourceFilter(sources));
        Set<ResourceRef> subjectAndAsserted = MoreSets.add(assertedAdjacents, subjAdjs.getRef());
        boolean change = !currentNeighbours.equals(subjectAndAsserted);
        if (change) {
            log().debug("Equivalence change: {} -> {}", currentNeighbours, subjectAndAsserted);
        }
        return change;
    }

    private Optional<EquivalenceGraph> existingGraph(ResourceRef subject) throws StoreException {
        return get(resolveIds(ImmutableSet.of(subject.getId()))).get(subject.getId());
    }

    private OptionalMap<Id, EquivalenceGraph> existingGraphMaps(Set<ResourceRef> adjacents)
            throws WriteException {
        return get(resolveIds(Iterables.transform(adjacents, Identifiables.toId())));
    }

    private <F> F get(ListenableFuture<F> resolved) throws WriteException {
        return Futures.get(resolved, TIMEOUT, TIMEOUT_UNITS, WriteException.class);
    }

    private ImmutableSet<EquivalenceGraph> store(ImmutableSet<EquivalenceGraph> graphs) {
        doStore(graphs);
        return graphs;
    }

    private static class CorruptEquivalenceGraphIndexException extends Exception {

        private final Id subjectId;

        public CorruptEquivalenceGraphIndexException(Id subjectId) {
            this.subjectId = checkNotNull(subjectId);
        }

        public Id getSubjectId() {
            return subjectId;
        }
    }
}
