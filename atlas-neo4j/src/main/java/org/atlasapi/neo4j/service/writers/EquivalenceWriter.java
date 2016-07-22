package org.atlasapi.neo4j.service.writers;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static com.google.common.base.Preconditions.checkArgument;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SOURCE;

public class EquivalenceWriter extends Neo4jWriter {

    private static final String SOURCE_ID = "sourceId";
    private static final String TARGET_ID = "targetId";
    private static final String ASSERTED_IDS = "assertedIds";
    private static final String SOURCES = "sources";

    private final Statement writeEdgeStatement;
    private final Statement removeNotAssertedEdgesStatement;

    private EquivalenceWriter() {
        writeEdgeStatement = new Statement(""
                + "MATCH "
                + "(sourceNode { " + CONTENT_ID + ": " + parameter(SOURCE_ID) + " }), "
                + "(targetNode { " + CONTENT_ID + ": " + parameter(TARGET_ID) + " }) "
                + "MERGE (sourceNode)-[r:IS_EQUIVALENT]->(targetNode)");

        removeNotAssertedEdgesStatement = new Statement(""
                + "MATCH (sourceNode { " + CONTENT_ID + ": " + parameter(SOURCE_ID) + " })"
                + "-[r:IS_EQUIVALENT]->(targetNode) "
                + "WHERE "
                + "NOT targetNode." + CONTENT_ID + " IN " + parameter(ASSERTED_IDS) + " "
                + "AND targetNode." + CONTENT_SOURCE + " IN " + parameter(SOURCES) + " "
                + "DELETE r");
    }

    public static EquivalenceWriter create() {
        return new EquivalenceWriter();
    }

    public void writeEquivalences(ResourceRef subject, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources, StatementRunner runner) {
        checkArgument(
                sources.contains(subject.getSource()),
                "Cannot update equivalences when subject resource source is not in the asserted "
                        + "sources. Subject: <" + subject + ">, "
                        + "sources: <" + Joiner.on(", ").join(sources) + ">."
        );

        // Add an edge from the subject to itself since a resource is always equivalent to itself
        ImmutableSet<ResourceRef> assertedAdjacentsPlusSubject = ImmutableSet.<ResourceRef>builder()
                .addAll(assertedAdjacents)
                .add(subject)
                .build();

        writeOutgoingEdges(
                subject.getId(),
                assertedAdjacentsPlusSubject,
                sources,
                runner
        );
    }

    private void writeOutgoingEdges(Id sourceNodeId, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources, StatementRunner runner) {
        ImmutableSet<Long> assertedAdjacentIds = assertedAdjacents
                .stream()
                .filter(resourceRef -> sources.contains(resourceRef.getSource()))
                .map(ResourceRef::getId)
                .map(Id::longValue)
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<String> assertedSources = sources.stream()
                .map(Publisher::key)
                .collect(MoreCollectors.toImmutableSet());

        removeNotAssertedEdges(
                sourceNodeId.longValue(), assertedAdjacentIds, assertedSources, runner
        );

        assertedAdjacentIds
                .forEach(assertedAdjacentId -> writeEdge(
                        sourceNodeId.longValue(), assertedAdjacentId, runner
                ));
    }

    private void writeEdge(Long sourceNodeId, Long targetNodeId, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                SOURCE_ID, sourceNodeId,
                TARGET_ID, targetNodeId
        );

        write(writeEdgeStatement.withParameters(statementParameters), runner);
    }

    private void removeNotAssertedEdges(Long sourceNodeId, ImmutableSet<Long> assertedAdjacentIds,
            Iterable<String> sources, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                SOURCE_ID, sourceNodeId,
                ASSERTED_IDS, assertedAdjacentIds,
                SOURCES, sources
        );

        write(removeNotAssertedEdgesStatement.withParameters(statementParameters), runner);
    }
}
