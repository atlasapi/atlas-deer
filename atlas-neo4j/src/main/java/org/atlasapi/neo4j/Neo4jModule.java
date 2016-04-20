package org.atlasapi.neo4j;

import org.atlasapi.neo4j.service.ContentGraphService;

import org.neo4j.ogm.session.Session;

public class Neo4jModule {

    private final Session session;

    private Neo4jModule() {
        this.session = Neo4jSessionFactory
                .createWithHttpDriver(
                        Neo4jSessionFactory.NEO4J_HOST,
                        Neo4jSessionFactory.NEO4J_PORT
                )
                .getNeo4jSession();
    }

    public static Neo4jModule create() {
        return new Neo4jModule();
    }

    public ContentGraphService contentGraphService() {
        return ContentGraphService.create(session);
    }

    public Session session() {
        return session;
    }
}
