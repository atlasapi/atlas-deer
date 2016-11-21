package org.atlasapi.equivalence;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.StoreException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.locks.GroupLock;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

public abstract class AbstractEquivalenceGraphStore implements EquivalenceGraphStore {

    private static final Logger log = LoggerFactory.getLogger(AbstractEquivalenceGraphStore.class);
    private static final int TIMEOUT = 1;
    private static final TimeUnit TIMEOUT_UNITS = TimeUnit.MINUTES;

    private static final String METER_CALLED = "meter.called";
    private static final String METER_FAILURE = "meter.failure";
    private static final String METER_NOP = "meter.nop";
    private static final String METER_BLACKLIST = "meter.blacklist";
    private static final String METER_FAILED_TO_LOCK_ADJACENTS = "lock.adjacents.meter.failure";
    private static final String METER_FAILED_TO_LOCK_TRANSITIVES = "lock.transitives.meter.failure";
    private static final String COUNTER_BLOCKED = "counter.blocked";
    private static final String HISTOGRAM_BLACKLIST = "histogram.blacklist";
    private static final String HISTOGRAM_LOCK_ATTEMPTS = "histogram.lockAttempts";
    private static final String TIMER_WAITING_LOCK = "timer.waitingLock";
    private static final String TIMER_EXECUTION = "timer.execution";
    private static final String CREATED_GRAPH_HISTOGRAM_COUNT = "graph.created.histogram.count";
    private static final String DELETED_GRAPH_HISTOGRAM_COUNT = "graph.deleted.histogram.count";
    private static final String UPDATED_GRAPH_HISTOGRAM_SIZE = "graph.updated.histogram.size";
    private static final String CREATED_GRAPH_HISTOGRAM_SIZE = "graph.created.histogram.size";

    private static final Duration EXECUTION_DURATION_ALERTING_THRESHOLD = Duration.ofSeconds(2);
    private static final int GRAPH_SIZE_ALERTING_THRESHOLD = 150;

    private final MessageSender<EquivalenceGraphUpdateMessage> messageSender;

    private final MetricRegistry metricRegistry;
    private final String updateEquivalences;

    private final Set<Id> blacklistedGraphAdjacents;

    public AbstractEquivalenceGraphStore(
            MessageSender<EquivalenceGraphUpdateMessage> messageSender,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        this.messageSender = checkNotNull(messageSender);

        this.metricRegistry = metricRegistry;
        this.updateEquivalences = metricPrefix + "updateEquivalences.";

        this.blacklistedGraphAdjacents = Sets.newHashSet(
                Id.valueOf(22036L),
                Id.valueOf(34790L),
                Id.valueOf(38300626L),
                Id.valueOf(42959503L),
                Id.valueOf(44245199L)
        );
        this.metricRegistry.histogram(updateEquivalences + HISTOGRAM_BLACKLIST)
                .update(blacklistedGraphAdjacents.size());
    }

    @Override
    public final Optional<EquivalenceGraphUpdate> updateEquivalences(
            ResourceRef subject,
            Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources
    ) throws WriteException {
        metricRegistry.meter(updateEquivalences + METER_CALLED).mark();
        Timer.Context executionTime = metricRegistry
                .timer(updateEquivalences + TIMER_EXECUTION)
                .time();

        ImmutableSet<Id> newAdjacents = assertedAdjacents.stream()
                .map(ResourceRef::getId)
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<Id> subjectAndAdjacents = MoreSets.add(newAdjacents, subject.getId());
        Set<Id> transitiveSetsIds = null;

        try {
            transitiveSetsIds = lockGraphIds(subjectAndAdjacents);

            Optional<EquivalenceGraphUpdate> updated = updateGraphs(
                    subject,
                    ImmutableSet.copyOf(assertedAdjacents),
                    sources
            );

            if (updated.isPresent()) {
                EquivalenceGraphUpdate graphUpdate = updated.get().copy()
                        .withAssertion(
                                EquivalenceAssertion.create(
                                        subject, assertedAdjacents, sources
                                )
                        )
                        .build();

                graphUpdate.getAllGraphs()
                        .stream()
                        .filter(graph ->
                                graph.getAdjacencyList().size() > GRAPH_SIZE_ALERTING_THRESHOLD)
                        .forEach(graph -> log.warn(
                                "Found large graph with id: {}, size: {}, update subject: {}",
                                graph.getId().longValue(),
                                graph.getAdjacencyList().size(),
                                subject
                        ));

                metricRegistry
                        .histogram(updateEquivalences + CREATED_GRAPH_HISTOGRAM_COUNT)
                        .update(graphUpdate.getCreated().size());

                metricRegistry
                        .histogram(updateEquivalences + DELETED_GRAPH_HISTOGRAM_COUNT)
                        .update(graphUpdate.getDeleted().size());

                metricRegistry
                        .histogram(updateEquivalences + UPDATED_GRAPH_HISTOGRAM_SIZE)
                        .update(graphUpdate.getUpdated().getAdjacencyList().size());

                graphUpdate.getCreated()
                        .forEach(graph -> metricRegistry
                                .histogram(updateEquivalences + CREATED_GRAPH_HISTOGRAM_SIZE)
                                .update(graph.getAdjacencyList().size()));

                updateBlacklist(graphUpdate);

                sendUpdateMessage(subject, graphUpdate);
            }
            return updated;

        } catch (InterruptedException e) {
            metricRegistry.meter(updateEquivalences + METER_FAILURE).mark();
            log.error(String.format("%s: %s", subject, newAdjacents), e);
            return Optional.absent();
        } catch (StoreException e) {
            metricRegistry.meter(updateEquivalences + METER_FAILURE).mark();
            Throwables.propagateIfPossible(e, WriteException.class);
            throw new WriteException(e);
        } catch (IllegalArgumentException e) {
            metricRegistry.meter(updateEquivalences + METER_FAILURE).mark();
            log.error(e.getMessage());
            return Optional.absent();
        } finally {
            unlock(subjectAndAdjacents, transitiveSetsIds);
            Duration executionDuration = Duration.ofNanos(executionTime.stop());

            if (executionDuration.compareTo(EXECUTION_DURATION_ALERTING_THRESHOLD) > 0) {
                log.warn(
                        "Slow update of equivalences with duration: {}, update subject: {}",
                        executionDuration,
                        subject
                );
            }
        }
    }

    protected abstract void doStore(ImmutableSet<EquivalenceGraph> graphs);

    protected abstract GroupLock<Id> lock();

    private void sendUpdateMessage(ResourceRef subject, EquivalenceGraphUpdate updatedGraphs) {
        try {
            messageSender.sendMessage(
                    new EquivalenceGraphUpdateMessage(
                            UUID.randomUUID().toString(),
                            Timestamp.of(DateTime.now(DateTimeZones.UTC)),
                            updatedGraphs
                    ),
                    Longs.toByteArray(updatedGraphs.getUpdated().getId().longValue())
            );
        } catch (MessagingException e) {
            log.warn("messaging failed for equivalence update of " + subject, e);
        }
    }

    private Set<Id> lockGraphIds(
            Set<Id> subjectAndAdjacents
    ) throws InterruptedException, StoreException {
        Timer.Context time = metricRegistry.timer(updateEquivalences + TIMER_WAITING_LOCK).time();
        metricRegistry.counter(updateEquivalences + COUNTER_BLOCKED).inc();

        try {
            Set<Id> transitiveSetsIds;

            log.debug(
                    "Thread {} is trying to enter synchronized block to lock graph IDs",
                    Thread.currentThread().getName()
            );

            int lockAttempts = 0;
            synchronized (lock()) {
                log.debug(
                        "Thread {} has entered synchronized block to lock graph IDs {}",
                        Thread.currentThread().getName(),
                        subjectAndAdjacents
                );

                while ((transitiveSetsIds = tryLockAllIds(subjectAndAdjacents)) == null) {
                    lockAttempts++;

                    lock().unlock(subjectAndAdjacents);
                    lock().wait(5000);
                    log.debug(
                            "Thread {} attempting to lock IDs {}",
                            Thread.currentThread().getName(),
                            subjectAndAdjacents
                    );
                }
            }
            log.debug(
                    "Thread {} has left synchronized block, having locked graph IDs",
                    Thread.currentThread().getName()
            );
            metricRegistry.histogram(updateEquivalences + HISTOGRAM_LOCK_ATTEMPTS)
                    .update(lockAttempts);

            return transitiveSetsIds;
        } finally {
            time.stop();
            metricRegistry.counter(updateEquivalences + COUNTER_BLOCKED).dec();
        }
    }

    @Nullable
    private Set<Id> tryLockAllIds(
            Set<Id> adjacentsIds
    ) throws InterruptedException, StoreException {
        checkBlacklist(adjacentsIds);

        if (!lock().tryLock(adjacentsIds)) {
            log.debug(
                    "Thread {} failed to lock adjacents {}",
                    Thread.currentThread().getName(),
                    adjacentsIds
            );
            metricRegistry.meter(updateEquivalences + METER_FAILED_TO_LOCK_ADJACENTS).mark();

            return null;
        }
        Iterable<Id> transitiveIds = transitiveIdsToLock(adjacentsIds);
        Set<Id> allIds = ImmutableSet.copyOf(Iterables.concat(transitiveIds, adjacentsIds));

        Iterable<Id> idsToLock = Iterables.filter(allIds, not(in(adjacentsIds)));
        boolean locked = lock().tryLock(ImmutableSet.copyOf(idsToLock));
        log.debug(
                "Thread {} Lock attempt success status {} locking transitive set {}",
                Thread.currentThread().getName(),
                locked,
                idsToLock
        );

        if (locked) {
            return allIds;
        }

        metricRegistry.meter(updateEquivalences + METER_FAILED_TO_LOCK_TRANSITIVES).mark();

        return null;
    }

    private Iterable<Id> transitiveIdsToLock(Set<Id> adjacentsIds) throws StoreException {
        return Iterables.concat(Iterables.transform(
                get(resolveIds(adjacentsIds)).values(),
                input -> input.isPresent()
                         ? input.get().getAdjacencyList().keySet()
                         : ImmutableSet.of()
        ));
    }

    private void unlock(Set<Id> subjectAndAdjacents, @Nullable Set<Id> transitiveSetsIds) {
        log.debug("Thread {} waiting for lock to unlock {} and {}",
                Thread.currentThread().getName(), subjectAndAdjacents, transitiveSetsIds
        );

        synchronized (lock()) {
            log.debug("Thread {} unlocking IDs {}",
                    Thread.currentThread().getName(), subjectAndAdjacents
            );

            lock().unlock(subjectAndAdjacents);

            if (transitiveSetsIds != null) {
                log.debug("Thread {} unlocking transitive IDs {}",
                        Thread.currentThread().getName(), transitiveSetsIds
                );

                lock().unlock(transitiveSetsIds);
            }
            log.debug(
                    "Thread {} performing notifyAll",
                    Thread.currentThread().getName(),
                    transitiveSetsIds
            );

            lock().notifyAll();
        }
    }

    private Optional<EquivalenceGraphUpdate> updateGraphs(ResourceRef subject,
            ImmutableSet<ResourceRef> assertedAdjacents, Set<Publisher> sources)
            throws StoreException {

        Optional<EquivalenceGraph> optionalSubjGraph = existingGraph(subject);
        EquivalenceGraph subjGraph = optionalSubjGraph.or(EquivalenceGraph.valueOf(subject));
        Adjacents subAdjs = subjGraph.getAdjacents(subject);

        OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraphs = existingGraphMaps(
                assertedAdjacents);

        if (optionalSubjGraph.isPresent() &&
                checkForOrphanedContent(subjGraph, adjacentsExistingGraphs, assertedAdjacents)) {
            log.warn(
                    "Found orphaned content in graph {}. Rewriting graph in store and retrying",
                    subjGraph.getId().longValue()
            );
            store(ImmutableSet.of(subjGraph));
            adjacentsExistingGraphs = existingGraphMaps(assertedAdjacents);
        }

        Map<ResourceRef, EquivalenceGraph> assertedAdjacentGraphs = resolveRefs(
                assertedAdjacents,
                adjacentsExistingGraphs
        );

        Map<Id, Adjacents> updatedAdjacents = updateAdjacencies(subject,
                subjGraph.getAdjacencyList().values(), assertedAdjacentGraphs, sources
        );

        EquivalenceGraphUpdate update =
                computeUpdate(subject, assertedAdjacentGraphs, updatedAdjacents);

        if (changeInAdjacents(subAdjs, assertedAdjacents, sources)) {
            store(update.getAllGraphs());

            return Optional.of(update);
        }

        metricRegistry.meter(updateEquivalences + METER_NOP).mark();
        log.debug("{}: no change in neighbours: {}", subject, assertedAdjacents);

        // Do not return an update if nothing has changed to reduce the work that the downstream
        // stores have to do.
        return Optional.absent();
    }

    private EquivalenceGraphUpdate computeUpdate(ResourceRef subject,
            Map<ResourceRef, EquivalenceGraph> assertedAdjacentGraphs,
            Map<Id, Adjacents> updatedAdjacents) {
        Map<Id, EquivalenceGraph> updatedGraphs = computeUpdatedGraphs(updatedAdjacents);
        EquivalenceGraph updatedGraph = graphFor(subject, updatedGraphs);

        return EquivalenceGraphUpdate.builder(updatedGraph)
                .withCreated(
                        Collections2.filter(
                                updatedGraphs.values(),
                                Predicates.not(Predicates.equalTo(updatedGraph))
                        )
                )
                .withDeleted(
                        Iterables.filter(
                                Iterables.transform(
                                        assertedAdjacentGraphs.values(),
                                        Identifiables.toId()
                                ),
                                Predicates.not(Predicates.in(updatedGraphs.keySet()))
                        )
                )
                .build();
    }

    private EquivalenceGraph graphFor(ResourceRef subject,
            Map<Id, EquivalenceGraph> updatedGraphs) {
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
        while (!work.isEmpty()) {
            Adjacents curr = work.poll();
            set.add(curr);
            work.addAll(Collections2.filter(
                    Collections2.transform(curr.getAdjacent(), toAdjs),
                    notSeen
            ));
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
                updated.put(
                        adj.getId(),
                        updateAdjacents(updated.get(adj.getId()),
                                subject,
                                adjacentGraphs.keySet(),
                                sources
                        )
                );
            } else {
                updated.put(
                        adj.getId(),
                        updateAdjacents(adj, subject, adjacentGraphs.keySet(), sources)
                );
            }
        }
        return ImmutableMap.copyOf(updated);
    }

    private Adjacents updateAdjacents(Adjacents adj, ResourceRef subject,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        Adjacents result = adj;
        if (subject.getId().equals(adj.getRef().getId())) {
            result = updateSubjectAdjacents(adj, assertedAdjacents, sources);
        } else if (sources.contains(adj.getSource())) {
            Set<Id> ids = assertedAdjacents
                    .stream()
                    .map(ResourceRef::getId)
                    .distinct()
                    .collect(MoreCollectors.toImmutableSet());
            if (ids.contains(adj.getRef().getId())) {
                result = adj.copyWithIncoming(subject);
            } else if (adj.hasIncomingAdjacent(subject)) {
                result = adj.copyWithoutIncoming(subject);
            }
        }
        return result;
    }

    private Adjacents updateSubjectAdjacents(Adjacents subj,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        ImmutableSet.Builder<ResourceRef> updatedOutgoingEdges = ImmutableSet.<ResourceRef>builder()
                .add(subj.getRef())
                .addAll(assertedAdjacents)
                .addAll(Sets.filter(
                        subj.getOutgoingEdges(),
                        Predicates.not(Sourceds.sourceFilter(sources))
                ));
        return subj.copyWithOutgoing(updatedOutgoingEdges.build());
    }

    private ImmutableSet.Builder<Adjacents> currentTransitiveAdjacents(
            Map<ResourceRef, EquivalenceGraph> resolved)
            throws StoreException {
        ImmutableSet.Builder<Adjacents> result = ImmutableSet.builder();
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
                        adjacent.getId()
                ));
    }

    private boolean contentIsOrphaned(EquivalenceGraph subjGraph,
            OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraphs, Id assertedAdjacentId) {
        return subjGraph.getEquivalenceSet().contains(assertedAdjacentId)
                && !adjacentsExistingGraphs.get(assertedAdjacentId).isPresent();
    }

    private Map<ResourceRef, EquivalenceGraph> resolveRefs(Set<ResourceRef> adjacents,
            OptionalMap<Id, EquivalenceGraph> adjacentsExistingGraph) {
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
                = Sets.filter(subjAdjs.getOutgoingEdges(), Sourceds.sourceFilter(sources));
        Set<ResourceRef> subjectAndAsserted = MoreSets.add(assertedAdjacents, subjAdjs.getRef());
        boolean change = !currentNeighbours.equals(subjectAndAsserted);
        if (change) {
            log.debug("Equivalence change: {} -> {}", currentNeighbours, subjectAndAsserted);
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

    private void checkBlacklist(Set<Id> adjacentsIds) {
        // Temporary blacklist to deal with queue backlog on bad graphs
        Sets.SetView<Id> intersection = Sets.intersection(adjacentsIds, blacklistedGraphAdjacents);

        if (intersection.isEmpty()) {
            return;
        }

        metricRegistry.meter(updateEquivalences + METER_BLACKLIST).mark();

        throw new IllegalArgumentException(String.format(
                "Blacklisted IDs %s found. Skipping...",
                intersection.stream()
                        .map(Id::longValue)
                        .sorted()
                        .map(Object::toString)
                        .collect(Collectors.joining(", "))
        ));
    }

    private void updateBlacklist(EquivalenceGraphUpdate graphUpdate) {
        graphUpdate.getAllGraphs()
                .stream()
                .filter(this::shouldAddToBlacklist)
                .flatMap(graph -> graph.getEquivalenceSet().stream())
                .forEach(blacklistedGraphAdjacents::add);

        metricRegistry.histogram(updateEquivalences + HISTOGRAM_BLACKLIST)
                .update(blacklistedGraphAdjacents.size());
    }

    private boolean shouldAddToBlacklist(EquivalenceGraph graph) {
        return !Sets.intersection(
                graph.getEquivalenceSet(),
                blacklistedGraphAdjacents
        )
                .isEmpty();
    }
}
