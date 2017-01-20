package org.atlasapi.application;

import com.codahale.metrics.MetricRegistry;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.ApplicationsClientImpl;
import com.metabroadcast.common.properties.Configurer;
import org.atlasapi.AtlasPersistenceModule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class })
public class ApplicationPersistenceModule {

    @Autowired AtlasPersistenceModule persistence;

    public ApplicationsClient applicationsClient() {
        return ApplicationsClientImpl.create(
                Configurer.get("applications.client.host").get(),
                new MetricRegistry()
        );
    }
}
