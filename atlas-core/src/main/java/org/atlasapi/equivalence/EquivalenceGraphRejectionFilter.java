package org.atlasapi.equivalence;

import org.atlasapi.entity.ResourceRef;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter inspects the size of the equivalence graph in the update and decides whether to
 * allow the update to be persisted or whether to reject it. This is to avoid downstream problems
 * in the equivalent content and schedule stores that are caused by very large graphs that are the
 * result of bugs in equivalence.
 * <p>
 * This filter has an alerting threshold and a rejection threshold.
 * <p>
 * If the graphs are over the alerting threshold it will emit a warning.
 * <p>
 * If they are also over the rejection threshold and the biggest graph in the update is bigger than
 * the biggest existing graph in that set then it will emit a rejection. This is to avoid
 * rejecting updates on already large graphs that would make them smaller.
 */
public class EquivalenceGraphRejectionFilter {

    private static final Logger log = LoggerFactory.getLogger(
            EquivalenceGraphRejectionFilter.class
    );

    private static final int GRAPH_SIZE_ALERTING_THRESHOLD = 150;
    private static final int GRAPH_SIZE_REJECTING_THRESHOLD = 250;

    private EquivalenceGraphRejectionFilter() {
    }

    public static EquivalenceGraphRejectionFilter create() {
        return new EquivalenceGraphRejectionFilter();
    }

    public Decision shouldReject(
            ResourceRef subject,
            ImmutableSet<EquivalenceGraph> existingGraphs,
            EquivalenceGraphUpdate graphUpdate
    ) {
        Decision decision = Decision.OK;

        ImmutableList<EquivalenceGraph> graphsOverWarningThreshold = getGraphsOverAlertingThreshold(
                subject,
                graphUpdate
        );

        if (!graphsOverWarningThreshold.isEmpty()) {
            decision = Decision.WARN;
        }

        if (shouldReject(existingGraphs, graphUpdate)) {
            decision = Decision.FAIL;
        }

        return decision;
    }

    private ImmutableList<EquivalenceGraph> getGraphsOverAlertingThreshold(
            ResourceRef subject,
            EquivalenceGraphUpdate graphUpdate
    ) {
        ImmutableList<EquivalenceGraph> graphsOverWarningThreshold = graphUpdate.getAllGraphs()
                .stream()
                .filter(graph ->
                        graph.getAdjacencyList().size() > GRAPH_SIZE_ALERTING_THRESHOLD)
                .collect(MoreCollectors.toImmutableList());

        graphsOverWarningThreshold
                .forEach(graph -> log.warn(
                        "Found large graph with id: {}, size: {}, update subject: {}",
                        graph.getId().longValue(),
                        graph.getAdjacencyList().size(),
                        subject
                ));

        return graphsOverWarningThreshold;
    }

    private boolean shouldReject(
            ImmutableSet<EquivalenceGraph> existingGraphs,
            EquivalenceGraphUpdate graphUpdate
    ) {
        int largestExistingGraph = existingGraphs.stream()
                .mapToInt(graph -> graph.getAdjacencyList().size())
                .max()
                .orElse(0);

        int largestUpdatedGraph = graphUpdate.getAllGraphs()
                .stream()
                .mapToInt(graph -> graph.getAdjacencyList().size())
                .max()
                .orElse(0);

        // Reject if the largest graph is greater than the threshold and if this update won't make
        // the largest of the pre-existing graphs smaller
        return largestUpdatedGraph > GRAPH_SIZE_REJECTING_THRESHOLD
                && largestUpdatedGraph >= largestExistingGraph;
    }

    public enum Decision {
        OK,
        WARN,
        FAIL
    }
}
