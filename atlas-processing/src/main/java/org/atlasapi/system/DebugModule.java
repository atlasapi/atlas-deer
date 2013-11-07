package org.atlasapi.system;

import org.atlasapi.persistence.AtlasPersistenceModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(AtlasPersistenceModule.class)
public class DebugModule {
    
    @Autowired private AtlasPersistenceModule persistenceModule;
    
    @Bean
    public ContentDebugController contentDebugController() {
        return new ContentDebugController(persistenceModule.contentStore());
    }
    
}
