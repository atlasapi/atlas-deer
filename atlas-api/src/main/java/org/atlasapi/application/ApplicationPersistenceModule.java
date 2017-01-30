package org.atlasapi.application;

import com.codahale.metrics.MetricRegistry;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.ApplicationsClientImpl;
import com.metabroadcast.common.properties.Configurer;
import com.netflix.discovery.converters.Auto;
import org.atlasapi.AtlasPersistenceModule;

import org.atlasapi.system.MetricsModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class, MetricsModule.class })
public class ApplicationPersistenceModule {

    @Autowired AtlasPersistenceModule persistence;
    @Autowired MetricRegistry metricRegistry;

    private static final String APPLICATIONS_CLIENT_HOST = "applications.client.host";

    public ApplicationsClient applicationsClient() {
        return ApplicationsClientImpl.create(
                Configurer.get(APPLICATIONS_CLIENT_HOST).get(),
                metricRegistry
        );
    }
}
