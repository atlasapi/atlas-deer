package org.atlasapi.neo4j;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class Neo4jSessionFactory {

    private final Driver driver;

    private Neo4jSessionFactory(Driver driver) {
        this.driver = checkNotNull(driver);
    }

    public static Neo4jSessionFactory create(
            String host, int port, AuthToken authToken, int maxIdleSessions
    ) {
        return new Neo4jSessionFactory(GraphDatabase
                .driver(
                        "bolt://" + host + ":" + port,
                        authToken,
                        Config.build()
                                .withMaxIdleSessions(maxIdleSessions)
                                .withEncryptionLevel(Config.EncryptionLevel.NONE)
                                .toConfig()
                ));
    }

    public Session getSession() {
        return driver.session();
    }

    public void close() {
        driver.close();
    }
}
