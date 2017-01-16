package org.atlasapi.application;

import com.codahale.metrics.MetricRegistry;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.ApplicationsClientImpl;
import com.metabroadcast.common.properties.Configurer;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.application.auth.MongoTokenRequestStore;

import com.metabroadcast.common.social.auth.credentials.CredentialsStore;
import com.metabroadcast.common.social.auth.credentials.MongoDBCredentialsStore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class })
public class ApplicationPersistenceModule {

    @Autowired AtlasPersistenceModule persistence;

    @Bean
    public CredentialsStore credentialsStore() {
        return new MongoDBCredentialsStore(persistence.databasedWriteMongo());
    }

    @Bean
    public MongoTokenRequestStore tokenStore() {
        return new MongoTokenRequestStore(persistence.databasedWriteMongo());
    }

    public ApplicationsClient applicationsClient() {
        return ApplicationsClientImpl.create(
                Configurer.get("applications.client.host").get(),
                new MetricRegistry()
        );
    }
}
