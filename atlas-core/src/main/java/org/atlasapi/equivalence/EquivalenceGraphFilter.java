package org.atlasapi.equivalence;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkArgument;

public class EquivalenceGraphFilter implements Predicate<Content> {

    private final ImmutableSet<Publisher> selectedSources;
    private final ImmutableSet<Id> selectedIds;

    private EquivalenceGraphFilter(Builder builder) {
        this.selectedSources = ImmutableSet.copyOf(builder.selectedSources);

        if (!builder.graph.isPresent() || !builder.graphEntryId.isPresent()) {
            this.selectedIds = ImmutableSet.copyOf(builder.activelyPublishedIds);
            return;
        }

        if (!builder.activelyPublishedIds.contains(builder.graphEntryId.get())) {
            this.selectedIds = ImmutableSet.of();
            return;
        }

        checkArgument(builder.graph.get()
                .getAdjacencyList()
                .containsKey(builder.graphEntryId.get()));

        this.selectedIds = visitId(
                builder.graphEntryId.get(),
                builder.graph.get(),
                builder.selectedGraphSources,
                builder.activelyPublishedIds,
                Sets.newHashSet()
        );
    }

    public static GraphEntryIdStep builder() {
        return new Builder();
    }

    @Override
    public boolean test(Content content) {
        return content.isActivelyPublished()
                && selectedSources.contains(content.getSource())
                && selectedIds.contains(content.getId());
    }

    private ImmutableSet<Id> visitId(
            Id graphEntryId,
            EquivalenceGraph graph,
            Set<Publisher> selectedSources,
            Set<Id> activelyPublishedIds,
            Set<Id> seenNodeIds
    ) {
        EquivalenceGraph.Adjacents adjacents = graph.getAdjacents(graphEntryId);

        ImmutableSet<ResourceRef> childrenToVisit = ImmutableSet.<ResourceRef>builder()
                .addAll(adjacents.getOutgoingEdges())
                .addAll(adjacents.getIncomingEdges())
                .build()
                .stream()
                .filter(ref -> !seenNodeIds.contains(ref.getId()))
                .collect(MoreCollectors.toImmutableSet());

        seenNodeIds.addAll(
                childrenToVisit.stream()
                        .map(ResourceRef::getId)
                        .collect(Collectors.toSet())
        );

        childrenToVisit.stream()
                .filter(ref -> activelyPublishedIds.contains(ref.getId()))
                .filter(ref -> selectedSources.contains(ref.getSource()))
                .forEach(ref -> visitId(
                        ref.getId(),
                        graph,
                        selectedSources,
                        activelyPublishedIds,
                        seenNodeIds
                ));

        return ImmutableSet.copyOf(seenNodeIds);
    }

    public interface GraphEntryIdStep {

        GraphStep withGraphEntryId(Optional<Id> graphEntryId);
    }

    public interface GraphStep {

        SelectedSourcesStep withGraph(Optional<EquivalenceGraph> graph);
    }

    public interface SelectedSourcesStep {

        SelectedGraphSourcesStep withSelectedSources(Set<Publisher> selectedSources);
    }

    public interface SelectedGraphSourcesStep {

        ActivelyPublishedIdsStep withSelectedGraphSources(Set<Publisher> selectedGraphSources);
    }

    public interface ActivelyPublishedIdsStep {

        BuildStep withActivelyPublishedIds(Set<Id> activelyPublishedIds);
    }

    public interface BuildStep {

        EquivalenceGraphFilter build();
    }

    public static class Builder
            implements GraphEntryIdStep, GraphStep, SelectedSourcesStep, SelectedGraphSourcesStep,
            ActivelyPublishedIdsStep, BuildStep {

        private Optional<Id> graphEntryId;
        private Optional<EquivalenceGraph> graph;
        private Set<Publisher> selectedSources;
        private Set<Publisher> selectedGraphSources;
        private Set<Id> activelyPublishedIds;

        private Builder() {
        }

        /**
         * The resource Id through which the filter should begin traversing the graph
         */
        @Override
        public GraphStep withGraphEntryId(Optional<Id> graphEntryId) {
            this.graphEntryId = graphEntryId;
            return this;
        }

        @Override
        public SelectedSourcesStep withGraph(Optional<EquivalenceGraph> graph) {
            this.graph = graph;
            return this;
        }

        /**
         * The sources for which we should return content. Any content with a source not on this
         * list will be filtered out
         */
        @Override
        public SelectedGraphSourcesStep withSelectedSources(Set<Publisher> selectedSources) {
            this.selectedSources = selectedSources;
            return this;
        }

        /**
         * The sources for which content we should traverse. Any content with a source not on this
         * list will not have its children in the graph traversed therefore if there is no path
         * to those children via another selected content then these children will be filtered out
         */
        @Override
        public ActivelyPublishedIdsStep withSelectedGraphSources(
                Set<Publisher> selectedGraphSources
        ) {
            this.selectedGraphSources = selectedGraphSources;
            return this;
        }

        /**
         * Ids for the graph content that is actively published. Non actively published content
         * will neither be returned by the filter nor will its children get traversed
         */
        @Override
        public BuildStep withActivelyPublishedIds(Set<Id> activelyPublishedIds) {
            this.activelyPublishedIds = activelyPublishedIds;
            return this;
        }

        @Override
        public EquivalenceGraphFilter build() {
            return new EquivalenceGraphFilter(this);
        }
    }
}
