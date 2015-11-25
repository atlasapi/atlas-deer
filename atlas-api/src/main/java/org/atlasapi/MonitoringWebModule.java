package org.atlasapi;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;

import org.atlasapi.system.HealthModule;
import org.atlasapi.system.JettyHealthProbe;
import org.atlasapi.system.MetricsModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
        HealthModule.class,
        MetricsModule.class,
        AtlasPersistenceModule.class
})
public class MonitoringWebModule {

    @Autowired
    private ServletContext servletContext;
    
    @PostConstruct
    public void setContext() {
        jettyHealthProbe().setServletContext(checkNotNull(servletContext));
    }
    
    @Bean 
    public JettyHealthProbe jettyHealthProbe() {
        return new JettyHealthProbe();
    }
    
}