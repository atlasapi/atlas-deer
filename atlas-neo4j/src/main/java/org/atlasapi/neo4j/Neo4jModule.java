package org.atlasapi.neo4j;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jModule {

    private final Neo4jSessionFactory sessionFactory;

    private Neo4jModule(Neo4jSessionFactory sessionFactory) {
        this.sessionFactory = checkNotNull(sessionFactory);
    }

    public static Neo4jModule create(String neo4jHost, int neo4jPort) {
        Neo4jSessionFactory sessionFactory = Neo4jSessionFactory.createWithHttpDriver(
                neo4jHost,
                neo4jPort,
                AuthTokens.none()
        );
        return new Neo4jModule(sessionFactory);
    }

    public Session session() {
        return sessionFactory.getSession();
    }
}
