package org.atlasapi.application;

import org.atlasapi.application.www.ApplicationWebModule;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ ApplicationPersistenceModule.class, ApplicationWebModule.class })
public class ApplicationModule {

}
