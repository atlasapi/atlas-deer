package org.atlasapi.neo4j;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jSessionFactory {

    private final Session session;

    private Neo4jSessionFactory(Driver driver) {
        checkNotNull(driver);
        this.session = driver.session();

        registerShutdownHook(driver, session);
    }

    public static Neo4jSessionFactory createWithHttpDriver(
            String host, int port, AuthToken authToken
    ) {
        return new Neo4jSessionFactory(GraphDatabase
                .driver(
                        "bolt://" + host + ":" + port,
                        authToken,
                        Config.build()
                                .withMaxIdleSessions(1)
                                .withEncryptionLevel(Config.EncryptionLevel.NONE)
                                .toConfig()
                ));
    }

    public Session getSession() {
        return session;
    }

    private void registerShutdownHook(Driver driver, Session session) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                session.close();
                driver.close();
            }
        });
    }
}
