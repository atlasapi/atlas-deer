package org.atlasapi.neo4j;

import org.junit.After;
import org.junit.Before;
import org.neo4j.driver.v1.Session;

public abstract class AbstractNeo4jIT {

    protected Neo4jModule module;
    protected Session session;

    @Before
    public void setUp() throws Exception {
        EmbeddedNeo4j.INSTANCE.checkAvailable();
        module = Neo4jModule.create(
                "localhost", EmbeddedNeo4j.BOLT_PORT, 1
        );
        session = module.sessionFactory().getSession();

        session.run("MATCH (n) DETACH DELETE n");
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        module.close();
    }
}
