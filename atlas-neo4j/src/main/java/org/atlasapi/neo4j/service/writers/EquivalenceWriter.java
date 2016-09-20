package org.atlasapi.neo4j.service.writers;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static com.google.common.base.Preconditions.checkArgument;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_SOURCE;
import static org.atlasapi.neo4j.service.model.Neo4jContent.IS_EQUIVALENT_RELATIONSHIP;

public class EquivalenceWriter extends Neo4jWriter {

    private static final String ORIGIN_ID_PARAM = "originId";
    private static final String TARGET_ID_PARAM = "targetId";
    private static final String ORIGIN_SOURCE_PARAM = "originSource";
    private static final String TARGET_SOURCE_PARAM = "targetSource";
    private static final String ASSERTED_IDS_PARAM = "assertedIds";
    private static final String SOURCES_PARAM = "sources";

    private final Statement writeEdgeStatement;
    private final Statement removeNotAssertedEdgesStatement;

    private EquivalenceWriter() {
        this.writeEdgeStatement = new Statement(""
                + "MERGE "
                + "(sourceNode { " + CONTENT_ID + ": " + param(ORIGIN_ID_PARAM) + " }) "
                + "ON CREATE SET "
                + "sourceNode." + CONTENT_SOURCE + " = " + param(ORIGIN_SOURCE_PARAM) + " "
                + "MERGE "
                + "(targetNode { " + CONTENT_ID + ": " + param(TARGET_ID_PARAM) + " }) "
                + "ON CREATE SET "
                + "targetNode." + CONTENT_SOURCE + " = " + param(TARGET_SOURCE_PARAM) + " "
                + "MERGE "
                + "(sourceNode)-[r:" + IS_EQUIVALENT_RELATIONSHIP + "]->(targetNode)");

        this.removeNotAssertedEdgesStatement = new Statement(""
                + "MATCH (sourceNode { " + CONTENT_ID + ": " + param(ORIGIN_ID_PARAM) + " })"
                + "-[r:" + IS_EQUIVALENT_RELATIONSHIP + "]->(targetNode) "
                + "WHERE "
                + "NOT targetNode." + CONTENT_ID + " IN " + param(ASSERTED_IDS_PARAM) + " "
                + "AND targetNode." + CONTENT_SOURCE + " IN " + param(SOURCES_PARAM) + " "
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
                subject,
                assertedAdjacentsPlusSubject,
                sources,
                runner
        );
    }

    private void writeOutgoingEdges(ResourceRef originNode, Set<ResourceRef> assertedAdjacents,
            Set<Publisher> sources, StatementRunner runner) {
        ImmutableList<ResourceRef> filteredAdjacents = assertedAdjacents
                .stream()
                .filter(resourceRef -> sources.contains(resourceRef.getSource()))
                .collect(MoreCollectors.toImmutableList());

        ImmutableSet<Long> assertedAdjacentIds = filteredAdjacents.stream()
                .map(ResourceRef::getId)
                .map(Id::longValue)
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<String> assertedSources = sources.stream()
                .map(Publisher::key)
                .collect(MoreCollectors.toImmutableSet());

        removeNotAssertedEdges(
                originNode.getId().longValue(), assertedAdjacentIds, assertedSources, runner
        );

        filteredAdjacents
                .forEach(adjacent -> writeEdge(
                        originNode, adjacent, runner
                ));
    }

    private void writeEdge(ResourceRef originNode, ResourceRef targetNode, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                ORIGIN_ID_PARAM, originNode.getId().longValue(),
                ORIGIN_SOURCE_PARAM, originNode.getSource().key(),
                TARGET_ID_PARAM, targetNode.getId().longValue(),
                TARGET_SOURCE_PARAM, targetNode.getSource().key()
        );

        write(writeEdgeStatement.withParameters(statementParameters), runner);
    }

    private void removeNotAssertedEdges(Long originNodeId, ImmutableSet<Long> assertedAdjacentIds,
            Iterable<String> sources, StatementRunner runner) {
        ImmutableMap<String, Object> statementParameters = ImmutableMap.of(
                ORIGIN_ID_PARAM, originNodeId,
                ASSERTED_IDS_PARAM, assertedAdjacentIds,
                SOURCES_PARAM, sources
        );

        write(removeNotAssertedEdgesStatement.withParameters(statementParameters), runner);
    }
}
