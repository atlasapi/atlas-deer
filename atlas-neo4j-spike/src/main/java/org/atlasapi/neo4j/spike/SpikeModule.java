package org.atlasapi.neo4j.spike;

import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jModule;

public class SpikeModule {

    private final Neo4jModule neo4jModule;
    private final ContentIndex contentIndex;
    private final ContentResolver contentResolver;
    private final EquivalenceGraphStore equivalenceGraphStore;

    public SpikeModule() {
        this.neo4jModule = new Neo4jModule();

        AtlasPersistenceModule atlasPersistenceModule = new AtlasPersistenceModule();
        this.contentIndex = atlasPersistenceModule.contentIndex();
        this.contentResolver = atlasPersistenceModule.contentResolver();
        this.equivalenceGraphStore = atlasPersistenceModule.equivalenceGraphStore();
    }

    public TestDateCreator testDateCreator(int limit, int maxOffset,
            Iterable<Publisher> publishers) {
        return new TestDateCreator(
                neo4jModule,
                contentIndex,
                contentResolver,
                equivalenceGraphStore,
                limit,
                maxOffset,
                publishers
        );
    }

}
