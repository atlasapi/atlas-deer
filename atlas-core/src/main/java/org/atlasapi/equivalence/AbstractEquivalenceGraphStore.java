package org.atlasapi.equivalence;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    private static final int TIMEOUT = 1;
    private static final TimeUnit TIMEOUT_UNITS = TimeUnit.MINUTES;
    
    private static final int maxSetSize = 150;
    
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
            synchronized (lock()) {
                while((transitiveSetsIds = tryLockAllIds(subjectAndAdjacents)) == null) {
                    lock().unlock(subjectAndAdjacents);
                    lock().wait();
                }
            }

            Optional<EquivalenceGraphUpdate> updated
                = updateGraphs(subject, ImmutableSet.<ResourceRef>copyOf(assertedAdjacents), sources);
            if (updated.isPresent()) {
                sendUpdateMessage(subject, updated);
            }
            return updated;
            
        } catch(OversizeTransitiveSetException otse) {
            log().info(String.format("Oversize set: %s + %s: %s", 
                subject, newAdjacents, otse.getMessage()));
            return Optional.absent();
        } catch(InterruptedException e) {
            log().error(String.format("%s: %s", subject, newAdjacents), e);
            return Optional.absent();
        } catch (StoreException e) {
            Throwables.propagateIfPossible(e, WriteException.class);
            throw new WriteException(e);
        } finally {
            synchronized (lock()) {
                lock().unlock(subjectAndAdjacents);
                if (transitiveSetsIds != null) {
                    lock().unlock(transitiveSetsIds);
                }
                lock().notifyAll();
            }
        }
        
    }

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
    
    protected abstract GroupLock<Id> lock();

    private Set<Id> tryLockAllIds(Set<Id> adjacentsIds) throws InterruptedException, StoreException {
        if (!lock().tryLock(adjacentsIds)) {
            return null;
        }
        Iterable<Id> transitiveIds = transitiveIdsToLock(adjacentsIds);
        Set<Id> allIds = ImmutableSet.copyOf(Iterables.concat(transitiveIds, adjacentsIds));
        if (allIds.size() > maxSetSize) {
            throw new OversizeTransitiveSetException(allIds.size());
        }
        Iterable<Id> idsToLock = Iterables.filter(allIds, not(in(adjacentsIds)));
        return lock().tryLock(ImmutableSet.copyOf(idsToLock)) ? allIds : null;
    }

    private Iterable<Id> transitiveIdsToLock(Set<Id> adjacentsIds) throws StoreException {
        return Iterables.concat(Iterables.transform(get(resolveIds(adjacentsIds)).values(),
            new Function<Optional<EquivalenceGraph>, Set<Id>>() {
                @Override
                public Set<Id> apply(Optional<EquivalenceGraph> input) {
                    return input.isPresent() ? input.get().getAdjacencyList().keySet() : ImmutableSet.<Id>of();
                }
            }
        ));
    }

    private Optional<EquivalenceGraphUpdate> updateGraphs(ResourceRef subject, 
            ImmutableSet<ResourceRef> assertedAdjacents, Set<Publisher> sources) throws StoreException {
        
        EquivalenceGraph subjGraph = existingGraph(subject).or(EquivalenceGraph.valueOf(subject));
        Adjacents subAdjs = subjGraph.getAdjacents(subject);
        
        checkState(subAdjs != null, "adjacents of %s not in graph %s", subject, subjGraph.getId());
        
        if(!changeInAdjacents(subAdjs, assertedAdjacents, sources)) {
            log().debug("{}: no change in neighbours: {}", subject, assertedAdjacents);
            return Optional.absent();
        }
        
        Map<ResourceRef, EquivalenceGraph> assertedAdjacentGraphs = resolveRefs(assertedAdjacents);
        
        Map<Id, Adjacents> updatedAdjacents = updateAdjacencies(subject,
                subjGraph.getAdjacencyList().values(), assertedAdjacentGraphs, sources);
        
        EquivalenceGraphUpdate update =
                computeUpdate(subject, assertedAdjacentGraphs, updatedAdjacents);
        
        store(update.getAllGraphs());
        
        return Optional.of(update);
    }

    private EquivalenceGraphUpdate computeUpdate(ResourceRef subject,
            Map<ResourceRef, EquivalenceGraph> assertedAdjacentGraphs, Map<Id, Adjacents> updatedAdjacents) {
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

    private Map<Id, EquivalenceGraph> computeUpdatedGraphs(Map<Id, Adjacents> updatedAdjacents) {
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

    private Set<Adjacents> transitiveSet(Adjacents adj, Function<Identifiable, Adjacents> toAdjs) {
        Set<Adjacents> set = Sets.newHashSet();
        Predicate<Adjacents> notSeen = Predicates.not(Predicates.in(set));
        
        Queue<Adjacents> work = Lists.newLinkedList();
        work.add(adj);
        while(!work.isEmpty()) {
            Adjacents curr = work.poll();
            set.add(curr);
            work.addAll(Collections2.filter(Collections2.transform(curr.getAdjacent(), toAdjs),notSeen));
        }
        return set;
    }

    private Map<Id, Adjacents> updateAdjacencies(ResourceRef subject,
            Iterable<Adjacents> subjAdjacencies, Map<ResourceRef, EquivalenceGraph> adjacentGraphs,
            Set<Publisher> sources) throws StoreException {
        ImmutableMap.Builder<Id, Adjacents> updated = ImmutableMap.builder();

        ImmutableSet<Adjacents> allAdjacents = currentTransitiveAdjacents(adjacentGraphs)
                .addAll(subjAdjacencies).build();
        for (Adjacents adj : allAdjacents) {
            updated.put(adj.getId(), updateAdjacents(adj, subject, adjacentGraphs.keySet(), sources));
        }
        return updated.build();
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

    private Map<ResourceRef, EquivalenceGraph> resolveRefs(Set<ResourceRef> adjacents)
            throws WriteException {
        OptionalMap<Id, EquivalenceGraph> existing = get(resolveIds(Iterables.transform(adjacents, Identifiables.toId())));
        Map<ResourceRef, EquivalenceGraph> graphs = Maps.newHashMapWithExpectedSize(adjacents.size());
        for (ResourceRef adj : adjacents) {
            graphs.put(adj, existing.get(adj.getId()).or(EquivalenceGraph.valueOf(adj)));
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

    private <F> F get(ListenableFuture<F> resolved) throws WriteException {
        return Futures.get(resolved, TIMEOUT, TIMEOUT_UNITS, WriteException.class);
    }

    private final ImmutableSet<EquivalenceGraph> store(ImmutableSet<EquivalenceGraph> graphs) {
        doStore(graphs);
        return graphs;
    }
    
    protected abstract void doStore(ImmutableSet<EquivalenceGraph> graphs);
    
    protected abstract Logger log();
    
    private static class OversizeTransitiveSetException extends RuntimeException {
        
        private int size;

        public OversizeTransitiveSetException(int size)  {
            this.size = size;
        }

        @Override
        public String getMessage() {
            return String.valueOf(size);
        }
        
    }
    
}
