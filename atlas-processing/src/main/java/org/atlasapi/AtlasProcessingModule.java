package org.atlasapi;

import com.metabroadcast.common.webapp.properties.ContextConfigurer;
import org.atlasapi.application.ApplicationModule;
import org.atlasapi.application.ApplicationPersistenceModule;
import org.atlasapi.application.www.ApplicationWebModule;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.WorkersModule;
import org.atlasapi.system.ProcessingHealthModule;
import org.atlasapi.system.ProcessingMetricsModule;
import org.atlasapi.system.bootstrap.BootstrapModule;
import org.atlasapi.system.debug.DebugModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        ProcessingHealthModule.class,
        ProcessingMetricsModule.class,
        KafkaMessagingModule.class,
        AtlasPersistenceModule.class,
        WorkersModule.class,
        BootstrapModule.class,
        ApplicationModule.class,
        ApplicationWebModule.class,
        ApplicationPersistenceModule.class,
        DebugModule.class
})
public class AtlasProcessingModule {

    @Bean
    public ContextConfigurer config() {
        ContextConfigurer c = new ContextConfigurer();
        c.init();
        return c;
    }

}
