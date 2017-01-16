package org.atlasapi.system;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SourcesModule {

    @Bean
    public SystemSourcesController systemSourcesControllergit () {
        return SystemSourcesController.create();
    }

}
