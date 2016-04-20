package org.atlasapi.neo4j.service.query;

public class EquivalentSetQuery {

    private static final int MAX_DEPTH = 8;

    private EquivalentSetQuery() { }

    public static EquivalentSetQuery create() {
        return new EquivalentSetQuery();
    }

    public String equivalentSet(String nodeName) {
        String query = "WITH " + "COLLECT(" + nodeName + ") AS res\n"
                + "WITH res, res AS front\n\n";

        for (int i = 0; i < MAX_DEPTH; i++) {
            query += equivalentSetStep("") + "\n";
        }

        query += "WITH res\n"
                + "UNWIND res AS " + nodeName + "\n"
                + "WITH DISTINCT " + nodeName + " AS " + nodeName + "\n";

        return query;
    }

    public String equivalentSetWithPassThrough(String nodeName, String passThroughNode) {
        String query = "WITH " + passThroughNode + ", COLLECT(" + nodeName + ") AS res\n"
                + "WITH " + passThroughNode + ", res, res AS front\n\n";

        for (int i = 0; i < MAX_DEPTH; i++) {
            query += equivalentSetStep(passThroughNode) + "\n";
        }

        query += "WITH " + passThroughNode + ", res\n"
                + "UNWIND res AS " + nodeName + "\n"
                + "WITH DISTINCT " + nodeName + " AS " + nodeName + ", " + passThroughNode + "\n";

        return query;
    }

    // Due to https://github.com/neo4j/neo4j/issues/6809, https://github.com/neo4j/neo4j/pull/6814
    // we need to do DISTINCT b and then COLLECT(b) because COLLECT(DISTINCT b) throws NPE
    private String equivalentSetStep(String passThroughVar) {
        String passThrough = passThroughVar.isEmpty() ? "" : passThroughVar + ", ";

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
