package org.atlasapi;

import org.atlasapi.application.ApplicationModule;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.query.QueryWebModule;
import org.atlasapi.system.HealthModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

@Configuration
@Import({
    AtlasPersistenceModule.class, 
    KafkaMessagingModule.class, 
    ApplicationModule.class,
    QueryWebModule.class,
    HealthModule.class
})
public class AtlasApiModule {

    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }

}
