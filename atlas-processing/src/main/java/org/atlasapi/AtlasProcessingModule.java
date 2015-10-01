package org.atlasapi;

import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.WorkersModule;
import org.atlasapi.messaging.temp.TempWorkersModule;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.bootstrap.BootstrapModule;
import org.atlasapi.system.debug.DebugModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;

@Configuration
@Import({
    ProcessingHealthModule.class,
    KafkaMessagingModule.class,
    AtlasPersistenceModule.class,
    WorkersModule.class,
    BootstrapModule.class,
    DebugModule.class,
        TempWorkersModule.class
})
public class AtlasProcessingModule {

    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }

}
