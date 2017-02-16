package org.atlasapi.content.v2;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class NormalizedEquivContentStore implements EquivalentContentStore {

    private final ContentResolver contentResolver;
    private final EquivalenceGraphStore graphStore;
    private final Timer resolveEquivTimer;
    private final Timer resolveIdsTimer;
    private final Counter resolveEquivCounter;
    private final Counter resolveIdsCounter;

    public NormalizedEquivContentStore(
            ContentResolver contentResolver,
            EquivalenceGraphStore graphStore,
            MetricRegistry metrics
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.graphStore = checkNotNull(graphStore);

        this.resolveEquivTimer = metrics.timer(String.format(
                "%s.resolveEquiv",
                getClass().getSimpleName()
        ));
        this.resolveEquivCounter = metrics.counter(String.format(
                "%s.resolveEquivCount",
                getClass().getSimpleName()
        ));
        this.resolveIdsTimer = metrics.timer(String.format(
                "%s.resolveIds",
                getClass().getSimpleName()
        ));
        this.resolveIdsCounter = metrics.counter(String.format(
                "%s.resolveIdsCount",
                getClass().getSimpleName()
        ));
    }

    @Override
    public void updateEquivalences(EquivalenceGraphUpdate update) throws WriteException {
        throw new UnsupportedOperationException("a wild Nope Pope appears");
    }

    @Override
    public void updateContent(Id contentId) throws WriteException {
        throw new UnsupportedOperationException("a wild Nope Pope appears");
    }

    @Override
    public ListenableFuture<Set<Content>> resolveEquivalentSet(Long equivalentSetId) {
        Timer.Context timer = resolveEquivTimer.time();

        ListenableFuture<OptionalMap<Id, EquivalenceGraph>> daGraph = graphStore.resolveIds(
                ImmutableList.of(Id.valueOf(equivalentSetId))
        );

        return Futures.transform(
                daGraph,
                (OptionalMap<Id, EquivalenceGraph> graph) -> {
                    Optional<EquivalenceGraph> resolvedGraph = Iterables.getOnlyElement(graph.values());
                    ImmutableSet<Id> equivSetIds = resolvedGraph.get().getEquivalenceSet();
                    ListenableFuture<Resolved<Content>> contentFuture =
                            contentResolver.resolveIds(equivSetIds);
                    try {
                        Resolved<Content> contentResolved = contentFuture.get();
                        return ImmutableSet.copyOf(contentResolved.getResources());
                    } catch (Exception e) {
                        throw Throwables.propagate(e);
                    } finally {
                        timer.stop();
                        resolveEquivCounter.inc();
                    }
                }
        );
    }

    @Override
    public ListenableFuture<ResolvedEquivalents<Content>> resolveIds(
            Iterable<Id> ids,
            Set<Publisher> selectedSources,
            Set<Annotation> activeAnnotations
    ) {
        Timer.Context timer = resolveIdsTimer.time();

        ListenableFuture<OptionalMap<Id, EquivalenceGraph>> equivSets = graphStore.resolveIds(ids);
        ListenableFuture<Set<Id>> toResolve = Futures.transform(
                equivSets,
                (OptionalMap<Id, EquivalenceGraph> eqSets) -> eqSets
                        .values()
                        .stream()
                        .flatMap(graph -> graph.get().getEquivalenceSet().stream())
                        .collect(Collectors.toSet())
        );

        ListenableFuture<Resolved<Content>> contentRequest = Futures.transform(
                toResolve,
                contentResolver::resolveIds
        );

        ListenableFuture<OptionalMap<Id, Content>> content = Futures.transform(
                contentRequest,
                (Function<Resolved<Content>, OptionalMap<Id, Content>>) Resolved::toMap
        );

        return Futures.transform(
                content,
                (OptionalMap<Id, Content> contentMap) -> {
                    ResolvedEquivalents.Builder<Content> result = ResolvedEquivalents.builder();

                    for (Id id : ids) {

                        Optional<EquivalenceGraph> equivSet;
                        try {
                            equivSet = equivSets.get().get(id);
                        } catch (InterruptedException | ExecutionException e) {
                            throw Throwables.propagate(e);
                        }

                        if (! equivSet.isPresent()) {
                            continue;
                        }

                        Set<Content> graphContent = equivSet.get()
                                .getEquivalenceSet()
                                .stream()
                                .map(contentMap::get)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .filter(c -> selectedSources.contains(c.getSource()))
                                .collect(MoreCollectors.toImmutableSet());

                        result.putEquivalents(id, graphContent);
                    }

                    timer.stop();
                    resolveIdsCounter.inc();

                    return result.build();
                }
        );

    }

    @Override
    public ListenableFuture<ResolvedEquivalents<Content>> resolveIdsWithoutEquivalence(
            Iterable<Id> ids,
            Set<Publisher> selectedSources,
            Set<Annotation> activeAnnotations
    ) {
        return resolveIds(ids, selectedSources, activeAnnotations);
    }
}
