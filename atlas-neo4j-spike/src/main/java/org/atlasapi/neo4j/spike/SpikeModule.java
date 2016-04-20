package org.atlasapi.neo4j.spike;

import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jModule;

import static com.google.common.base.Preconditions.checkNotNull;

public class SpikeModule {

    private final Neo4jModule neo4jModule;
    private final ContentIndex contentIndex;
    private final ContentResolver contentResolver;
    private final EquivalenceGraphStore equivalenceGraphStore;

    private SpikeModule(
            ContentIndex contentIndex,
            ContentResolver contentResolver,
            EquivalenceGraphStore equivalenceGraphStore
    ) {
        this.neo4jModule = Neo4jModule.create();

        this.contentIndex = checkNotNull(contentIndex);
        this.contentResolver = checkNotNull(contentResolver);
        this.equivalenceGraphStore = checkNotNull(equivalenceGraphStore);
    }

    public static SpikeModule createStandalone() {
        Neo4jAtlasPersistenceModule atlasPersistenceModule = new Neo4jAtlasPersistenceModule();
        return new SpikeModule(
                atlasPersistenceModule.contentIndex(),
                atlasPersistenceModule.contentResolver(),
                atlasPersistenceModule.equivalenceGraphStore()
        );
    }

    public static SpikeModule create(
            ContentIndex contentIndex,
            ContentResolver contentResolver,
            EquivalenceGraphStore equivalenceGraphStore
    ) {
        return new SpikeModule(contentIndex, contentResolver, equivalenceGraphStore);
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
