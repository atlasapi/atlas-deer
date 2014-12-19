package org.atlasapi;

import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.WorkersModule;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.BootstrapModule;
import org.atlasapi.system.debug.DebugModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

@Configuration
@Import({
    KafkaMessagingModule.class,
    AtlasPersistenceModule.class, 
    WorkersModule.class,
    ProcessingHealthModule.class,
    BootstrapModule.class,
    DebugModule.class,
})
public class AtlasProcessingModule {

    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }

}
