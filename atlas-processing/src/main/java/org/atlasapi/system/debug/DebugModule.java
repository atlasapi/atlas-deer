package org.atlasapi.system.debug;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class })
public class DebugModule {

    @Autowired
    private AtlasPersistenceModule persistenceModule;

    @Bean
    public ContentDebugController contentDebugController() {
        return ContentDebugController.builder()
                .withLegacyContentResolver(persistenceModule.legacyContentResolver())
                .withLegacySegmentMigrator(persistenceModule.legacySegmentMigrator())
                .withPersistence(persistenceModule)
                .withEquivalenceMigrator(explicitEquivalenceMigrator())
                .withIndex(persistenceModule.contentIndex())
                .withEsContentTranslator(persistenceModule.esContentTranslator())
                .withNeo4jContentStore(persistenceModule.neo4jContentStore())
                .withContentStore(persistenceModule.contentStore())
                .withContentEquivalenceGraphStore(persistenceModule.getContentEquivalenceGraphStore())
                .withEquivalentContentStore(persistenceModule.getEquivalentContentStore())
                .build();
    }

    @Bean
    public EventDebugController eventDebugController() {
        return new EventDebugController(persistenceModule.legacyEventResolver(), persistenceModule);
    }

    @Bean
    public ScheduleDebugController scheduleDebugController() {
        return new ScheduleDebugController(
                persistenceModule.channelResolver(),
                persistenceModule.getEquivalentScheduleStore(),
                persistenceModule.scheduleStore());
    }

    @Bean
    public DelphiController delphiController() {
        return DelphiController.create(
                persistenceModule.getContentEquivalenceGraphStore()
        );
    }

    private DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new DirectAndExplicitEquivalenceMigrator(
                persistenceModule.legacyContentResolver(),
                persistenceModule.legacyEquivalenceStore(),
                persistenceModule.nullMessageSendingGraphStore()
        );
    }
}
