package org.atlasapi.neo4j;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class Neo4jModuleIT extends AbstractNeo4jIT {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() throws Exception {
        session.run("CREATE (a:Person {name:'Arthur', title:'King'})");

        StatementResult queryResult = session.run(""
                + "MATCH (a:Person) "
                + "WHERE a.name = 'Arthur' "
                + "RETURN a.name AS name, a.title AS title");

        assertThat(queryResult.hasNext(), is(true));

        Record record = queryResult.next();
        assertThat(record.get("name").asString(), is("Arthur"));
        assertThat(record.get("title").asString(), is("King"));
    }
}
