package org.atlasapi.neo4j;

import org.neo4j.ogm.config.Configuration;
import org.neo4j.ogm.service.Components;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jSessionFactory {

    public static final String NEO4J_HOST = "http://neo4j.stage.mbst.tv";
    public static final int NEO4J_PORT = 7474;

    private static final String MODEL_PACKAGE = "org.atlasapi.neo4j.model";

    private final SessionFactory sessionFactory;

    private Neo4jSessionFactory(Configuration configuration) {
        sessionFactory = new SessionFactory(checkNotNull(configuration), MODEL_PACKAGE);
    }

    public static Neo4jSessionFactory createWithHttpDriver(String host, int port) {
        Configuration configuration = Components.configuration();

        configuration.driverConfiguration()
                .setDriverClassName("org.neo4j.ogm.drivers.http.driver.HttpDriver")
                .setURI(host + ":" + port);

        return new Neo4jSessionFactory(configuration);
    }

    public static Neo4jSessionFactory createWithEmbeddedDriver() {
        Configuration configuration = Components.configuration();

        configuration.driverConfiguration()
                .setDriverClassName("org.neo4j.ogm.drivers.embedded.driver.EmbeddedDriver");

        return new Neo4jSessionFactory(configuration);
    }

    public Session getNeo4jSession() {
        return sessionFactory.openSession();
    }
}
