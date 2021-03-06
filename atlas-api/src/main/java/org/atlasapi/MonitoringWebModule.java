package org.atlasapi;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;

import org.atlasapi.system.JettyHealthProbe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.google.common.base.Preconditions.checkNotNull;

@Configuration
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