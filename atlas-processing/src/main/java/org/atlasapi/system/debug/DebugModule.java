package org.atlasapi.system.debug;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.legacy.LegacyPersistenceModule;
import org.atlasapi.system.bootstrap.workers.ExplicitEquivalenceMigrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class, LegacyPersistenceModule.class })
public class DebugModule {

    @Autowired
    private AtlasPersistenceModule persistenceModule;
    @Autowired
    private LegacyPersistenceModule legacyPersistenceModule;

    @Bean
    public ContentDebugController contentDebugController() {
        return new ContentDebugController(
                legacyPersistenceModule.legacyContentResolver(),
                persistenceModule.getEquivalentContentStore(),
                persistenceModule.contentStore(),
                explicitEquivalenceMigrator());
    }

    public ExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new ExplicitEquivalenceMigrator(
                legacyPersistenceModule.legacyContentResolver(),
                legacyPersistenceModule.legacyEquivalenceStore(),
                persistenceModule.getContentEquivalenceGraphStore()
        );
    }

}
