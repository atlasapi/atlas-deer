package org.atlasapi.neo4j;

import java.io.File;

import com.google.common.io.Files;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static com.google.common.base.Preconditions.checkArgument;

public enum EmbeddedNeo4j {
    INSTANCE;

    public static final int BOLT_PORT = 7687;

    private final GraphDatabaseService graphDb;

    EmbeddedNeo4j() {
        File dbDirectory = Files.createTempDir();

        GraphDatabaseSettings.BoltConnector bolt = GraphDatabaseSettings.boltConnector("bolt");

        this.graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbDirectory)
                .setConfig(bolt.enabled, "true")
                .setConfig(bolt.address, "localhost:" + BOLT_PORT)
                .setConfig(bolt.encryption_level, "DISABLED")
                .setConfig(bolt.type, "BOLT")
                .newGraphDatabase();

        dbDirectory.deleteOnExit();
        registerShutdownHook(this.graphDb);
    }

    public void checkAvailable() {
        checkArgument(graphDb.isAvailable(500L), "Embedded neo4j is not ready");
    }

    private void registerShutdownHook(GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
