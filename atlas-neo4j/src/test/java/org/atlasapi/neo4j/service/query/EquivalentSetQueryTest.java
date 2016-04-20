package org.atlasapi.neo4j.service.query;

import org.junit.Before;
import org.junit.Test;

public class EquivalentSetQueryTest {

    private EquivalentSetQuery equivalentSetQuery;

    @Before
    public void setUp() throws Exception {
        equivalentSetQuery = EquivalentSetQuery.create();
    }

    @Test
    public void equivalentSet() throws Exception {
        String query = "MATCH (n:Brand { id: 7923 })\n\n"
                + equivalentSetQuery.equivalentSet("n")
                + "\n"
                + "RETURN n";

        // TODO Test properly
        System.out.println(query);
    }

    @Test
    public void equivalentSetWithPassThrough() throws Exception {
        String query = "MATCH (n:Brand { id: 7923 })\n\n"
                + "WITH n, n AS matched_n\n\n"
                + equivalentSetQuery.equivalentSetWithPassThrough("n", "matched_n")
                + "\n"
                + "RETURN matched_n, n";

        // TODO Test properly
        System.out.println(query);
    }
}
