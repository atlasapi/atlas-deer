package org.atlasapi.application;

import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.ApplicationsClientImpl;
import com.metabroadcast.common.properties.Configurer;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.system.MetricsModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static com.google.common.base.Preconditions.checkNotNull;

@Configuration
@Import({ AtlasPersistenceModule.class })
public class ApplicationPersistenceModule {

    private final String applicationsClientHost = checkNotNull(Configurer.get("applications.client.host").get());

    @Autowired AtlasPersistenceModule persistence;
    private @Autowired MetricsModule metricsModule;

    public ApplicationsClient applicationsClient() {
        try {
            Thread.sleep(2 * 60 * 100);
        } catch (InterruptedException e) {
        }
        return ApplicationsClientImpl.create(
                applicationsClientHost,
                metricsModule.metrics()
        );
    }
}
