package org.atlasapi;

import org.atlasapi.application.ApplicationModule;
import org.atlasapi.application.InternalClientsModule;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.query.QueryWebModule;
import org.atlasapi.system.HealthModule;
import org.atlasapi.system.MetricsModule;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

import org.atlasapi.system.SourcesModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        HealthModule.class,
        MetricsModule.class,
        AtlasPersistenceModule.class,
        KafkaMessagingModule.class,
        ApplicationModule.class,
        QueryWebModule.class,
        SourcesModule.class,
        InternalClientsModule.class,
})
public class AtlasApiModule {

    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }
}
