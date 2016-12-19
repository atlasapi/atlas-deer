package org.atlasapi.system.debug;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.query.v4.content.v2.CqlContentDebugController;
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
        return new ContentDebugController(
                persistenceModule.legacyContentResolver(),
                persistenceModule.legacySegmentMigrator(),
                persistenceModule,
                explicitEquivalenceMigrator(),
                persistenceModule.contentIndex(),
                persistenceModule.esContentTranslator(),
                persistenceModule.neo4jContentStore(),
                persistenceModule.contentStore(),
                persistenceModule.getContentEquivalenceGraphStore(),
                persistenceModule.getEquivalentContentStore()
        );
    }

    @Bean
    CqlContentDebugController cqlController() {
        return new CqlContentDebugController(
                persistenceModule.legacyContentResolver(),
                persistenceModule.contentStore(),
                persistenceModule.cqlContentStore()
        );
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
