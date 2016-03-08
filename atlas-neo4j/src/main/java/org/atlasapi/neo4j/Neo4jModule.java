package org.atlasapi.neo4j;

import org.atlasapi.neo4j.service.SpikeContentNodeService;

import org.neo4j.ogm.session.Session;

public class Neo4jModule {

    private final Session session;

    public Neo4jModule() {
        this.session = Neo4jSessionFactory.createWithHttpDriver().getNeo4jSession();
    }

    public SpikeContentNodeService spikeContentNodeService() {
        return SpikeContentNodeService.create(session);
    }

    public Session session() {
        return session;
    }
}
