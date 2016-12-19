package org.atlasapi.application;

import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.ApplicationsClientImpl;
import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.application.auth.MongoTokenRequestStore;
import org.atlasapi.application.users.LegacyAdaptingUserStore;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.application.users.v3.MongoUserStore;
import org.atlasapi.application.v3.MongoApplicationStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.users.videosource.MongoUserVideoSourceStore;
import org.atlasapi.users.videosource.UserVideoSourceStore;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.social.auth.credentials.CredentialsStore;
import com.metabroadcast.common.social.auth.credentials.MongoDBCredentialsStore;

import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ AtlasPersistenceModule.class })
public class ApplicationPersistenceModule {

    private int cacheMinutes = Integer.parseInt(Configurer.get("application.cache.minutes").get());

    @Autowired AtlasPersistenceModule persistence;

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    @Bean
    public CredentialsStore credentialsStore() {
        return new MongoDBCredentialsStore(persistence.databasedWriteMongo());
    }

    @Bean
    public MongoTokenRequestStore tokenStore() {
        return new MongoTokenRequestStore(persistence.databasedWriteMongo());
    }

    public ApplicationsClient applicationsClient() { //TODO: Make this work
        return ApplicationsClientImpl.create(
                "someHost",
                null,
                "applicationsClient"
        );
    }

    @Bean
    public SourceRequestStore sourceRequestStore() {
        return new MongoSourceRequestStore(persistence.databasedWriteMongo());
    }

    public
    @Bean
    UserStore userStore() {
        MongoUserStore legacy = new MongoUserStore(persistence.databasedWriteMongo());
        return new LegacyAdaptingUserStore(
                legacy,
                persistence.databasedWriteMongo()
        );
    }

    @Bean
    public UserVideoSourceStore linkedOauthTokenUserStore() {
        return new MongoUserVideoSourceStore(persistence.databasedWriteMongo());
    }

    @Bean
    public SourceLicenseStore sourceLicenseStore() {
        return new MongoSourceLicenseStore(persistence.databasedWriteMongo());
    }

    @Bean
    public EndUserLicenseStore endUserLicenseStore() {
        return new MongoEndUserLicenseStore(persistence.databasedWriteMongo());
    }
}
