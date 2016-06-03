package org.atlasapi.system.debug;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.bootstrap.workers.DirectAndExplicitEquivalenceMigrator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class})
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
                persistenceModule.esContentTranslator()
        );
    }

    @Bean
    public EventDebugController eventDebugController() {
        return new EventDebugController(persistenceModule.legacyEventResolver(), persistenceModule);
    }

    public DirectAndExplicitEquivalenceMigrator explicitEquivalenceMigrator() {
        return new DirectAndExplicitEquivalenceMigrator(
                persistenceModule.legacyContentResolver(),
                persistenceModule.legacyEquivalenceStore(),
                persistenceModule.nullMessageSendingGraphStore()
        );
    }

    @Bean
    public ScheduleDebugController scheduleDebugController() {
        return new ScheduleDebugController(
                persistenceModule.channelResolver(),
                persistenceModule.getEquivalentScheduleStore(),
                persistenceModule.scheduleStore());
    }
}
