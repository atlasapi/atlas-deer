package org.atlasapi.neo4j.service.resolvers;

import java.util.List;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;

public class EquivalentSetResolver extends Neo4jResolver {

    /**
     * Max depth to search during BFS
     */
    @VisibleForTesting
    static final int MAX_DEPTH = 8;

    private static final String ID_PARAM = "id";

    private final Statement resolveEquivalenceGraphStatement;

    private EquivalentSetResolver() {
        this.resolveEquivalenceGraphStatement = new Statement(""
                + "MATCH (content {" + CONTENT_ID + ": " + parameter(ID_PARAM) + "})\n"
                + equivalentSet("content", "")
                + "RETURN content.id AS content");
    }

    public static EquivalentSetResolver create() {
        return new EquivalentSetResolver();
    }

    public ImmutableSet<Id> getEquivalentSet(Id id, StatementRunner runner) {
        List<Record> records = read(
                resolveEquivalenceGraphStatement.withParameters(ImmutableMap.of(
                        ID_PARAM, id.longValue()
                )),
                runner
        );

        return records.stream()
                .map(record -> record.get("content").asLong())
                .map(Id::valueOf)
                .collect(MoreCollectors.toImmutableSet());
    }

    private String equivalentSet(String nodeName, String passThroughNode) {
        String passThrough = passThroughNode.isEmpty() ? "" : passThroughNode + ", ";

        String query = "WITH " + passThrough + "COLLECT(" + nodeName + ") AS res\n"
                + "WITH " + passThrough + "res, res AS front\n\n";

        for (int i = 0; i < MAX_DEPTH; i++) {
            query += equivalentSetStep(passThrough) + "\n";
        }

        query += "WITH " + passThrough + "res\n"
                + "UNWIND res AS " + nodeName + "\n"
                + "WITH " + passThrough + "DISTINCT " + nodeName + " AS " + nodeName + "\n";

        return query;
    }

    // Due to https://github.com/neo4j/neo4j/issues/6809, https://github.com/neo4j/neo4j/pull/6814
    // we need to do DISTINCT b and then COLLECT(b) because COLLECT(DISTINCT b) throws NPE
    private String equivalentSetStep(String passThrough) {
        return "UNWIND front AS a\n"
                + "OPTIONAL MATCH (a)-[:IS_EQUIVALENT]-(b)\n"
                + "WHERE NOT b in res\n"
                + "WITH DISTINCT b, " + passThrough + "res\n"
                + "WITH " + passThrough + "res, COALESCE(COLLECT(b),[]) AS front\n"
                + "WITH " + passThrough + "front, res + front AS res\n"
                + "WITH " + passThrough + "res, "
                + "CASE front WHEN [] THEN [NULL] ELSE front END AS front\n";
    }
}


