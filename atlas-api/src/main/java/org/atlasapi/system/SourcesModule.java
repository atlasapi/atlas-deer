package org.atlasapi.system;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SourcesModule {

    private @Autowired SourcesController sourcesController;

    @Bean
    public SourcesController sourcesController() {
        return SourcesController.create();
    }

}
