package org.atlasapi.application;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.application.auth.MongoTokenRequestStore;
import org.atlasapi.application.users.LegacyAdaptingUserStore;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.application.users.v3.MongoUserStore;
import org.atlasapi.application.v3.MongoApplicationStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.users.videosource.MongoUserVideoSourceStore;
import org.atlasapi.users.videosource.UserVideoSourceStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.social.auth.credentials.CredentialsStore;
import com.metabroadcast.common.social.auth.credentials.MongoDBCredentialsStore;
import com.mongodb.ReadPreference;

@Configuration
@Import({AtlasPersistenceModule.class})
public class ApplicationPersistenceModule {
    
    private int cacheMinutes = Integer.parseInt(Configurer.get("application.cache.minutes").get());
    
    @Autowired AtlasPersistenceModule persistence;
    
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    @Bean
    public CredentialsStore credentialsStore() {
        return new MongoDBCredentialsStore(persistence.databasedMongo());
    }
    
    @Bean
    public MongoTokenRequestStore tokenStore() {
        return new MongoTokenRequestStore(persistence.databasedMongo());
    }
    
    @Bean
    public LegacyApplicationStore applicationStore() {
        DatabasedMongo mongo = persistence.databasedMongo();
        IdGenerator idGenerator = new MongoSequentialIdGenerator(mongo, "application");
        MongoApplicationStore legacyStore = new MongoApplicationStore(mongo, idGenerator, ReadPreference.primaryPreferred());
        LegacyAdaptingApplicationStore store = new LegacyAdaptingApplicationStore(legacyStore, mongo, idGenerator, idCodec);
        return new CacheBackedApplicationStore(store, cacheMinutes);
    }
    
    @Bean
    public SourceRequestStore sourceRequestStore() {
        return new MongoSourceRequestStore(persistence.databasedMongo());
    }
    
    public @Bean
    UserStore userStore() {
        MongoUserStore legacy = new MongoUserStore(persistence.databasedMongo());
        return new LegacyAdaptingUserStore(legacy, applicationStore(), persistence.databasedMongo());
    }
    
    @Bean
    public UserVideoSourceStore linkedOauthTokenUserStore() {
    	return new MongoUserVideoSourceStore(persistence.databasedMongo());
    }
    
    @Bean
    public SourceLicenseStore sourceLicenseStore() {
        return new MongoSourceLicenseStore(persistence.databasedMongo());
    }
    
    @Bean 
    public EndUserLicenseStore endUserLicenseStore() {
        return new MongoEndUserLicenseStore(persistence.databasedMongo());
    }
}
