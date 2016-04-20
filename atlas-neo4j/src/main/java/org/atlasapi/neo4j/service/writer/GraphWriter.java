package org.atlasapi.neo4j.service.writer;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;

import com.google.common.collect.ImmutableMap;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class GraphWriter {

    private final Session session;

    private GraphWriter(Session session) {
        this.session = checkNotNull(session);
    }

    public static GraphWriter create(Session session) {
        return new GraphWriter(session);
    }

    public void writeGraph(EquivalenceGraph graph) {
        graph.getAdjacencyList().entrySet().stream()
                .forEach(entry -> writeEquivalenceRelationships(
                        entry.getKey(), entry.getValue()
                ));
    }

    private void writeEquivalenceRelationships(Id sourceNode, EquivalenceGraph.Adjacents adjacents) {
        adjacents.getEfferent().stream()
                .forEach(resourceRef -> writeEquivalenceRelationship(
                        sourceNode, resourceRef.getId())
                );
    }

    private void writeEquivalenceRelationship(Id sourceNode, Id targetNode) {
        String query = "MATCH (source { id: {sourceId} }), (target {id: {targetId} })\n"
                + "MERGE (source)-[r:IS_EQUIVALENT]->(target)\n"
                + "RETURN id(r)";

        session.query(query, ImmutableMap.of(
                "sourceId", sourceNode.longValue(),
                "targetId", targetNode.longValue()
        ));
    }
}
