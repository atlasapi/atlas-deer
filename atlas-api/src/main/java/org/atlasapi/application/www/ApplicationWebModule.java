package org.atlasapi.application.www;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationPersistenceModule;
import org.atlasapi.application.ApplicationQueryExecutor;
import org.atlasapi.application.ApplicationsController;
import org.atlasapi.application.AtlasPersistenceModule;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceRequest;
import org.atlasapi.application.SourceRequestManager;
import org.atlasapi.application.SourceRequestQueryExecutor;
import org.atlasapi.application.SourceRequestsController;
import org.atlasapi.application.SourcesController;
import org.atlasapi.application.SourcesQueryExecutor;
import org.atlasapi.application.auth.ApiKeySourcesFetcher;
import org.atlasapi.application.auth.ApplicationSourcesFetcher;
import org.atlasapi.application.auth.AuthProvidersListWriter;
import org.atlasapi.application.auth.AuthProvidersQueryResultWriter;
import org.atlasapi.application.auth.OAuthInterceptor;
import org.atlasapi.application.auth.OAuthRequestListWriter;
import org.atlasapi.application.auth.OAuthRequestQueryResultWriter;
import org.atlasapi.application.auth.OAuthResultListWriter;
import org.atlasapi.application.auth.OAuthResultQueryResultWriter;
import org.atlasapi.application.auth.OAuthTokenUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.auth.twitter.TwitterAuthController;
import org.atlasapi.application.auth.www.AuthController;
import org.atlasapi.application.model.deserialize.IdDeserializer;
import org.atlasapi.application.model.deserialize.PublisherDeserializer;
import org.atlasapi.application.model.deserialize.RoleDeserializer;
import org.atlasapi.application.model.deserialize.SourceReadEntryDeserializer;
import org.atlasapi.application.notification.NotifierModule;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.NewUserSupplier;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UsersController;
import org.atlasapi.application.users.UsersQueryExecutor;
import org.atlasapi.application.writers.ApplicationListWriter;
import org.atlasapi.application.writers.ApplicationQueryResultWriter;
import org.atlasapi.application.writers.SourceRequestListWriter;
import org.atlasapi.application.writers.SourceRequestsQueryResultsWriter;
import org.atlasapi.application.writers.SourceWithIdWriter;
import org.atlasapi.application.writers.SourcesQueryResultWriter;
import org.atlasapi.application.writers.UsersListWriter;
import org.atlasapi.application.writers.UsersQueryResultWriter;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.input.GsonModelReader;
import org.atlasapi.input.ModelReader;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.AttributeCoercers;
import org.atlasapi.query.common.IndexAnnotationsExtractor;
import org.atlasapi.query.common.QueryAtomParser;
import org.atlasapi.query.common.QueryAttributeParser;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParser;
import org.atlasapi.query.common.useraware.UserAwareQueryContextParser;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.mvc.annotation.DefaultAnnotationHandlerMapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.social.auth.facebook.AccessTokenChecker;
import com.metabroadcast.common.social.auth.facebook.CachingAccessTokenChecker;
import com.metabroadcast.common.social.twitter.TwitterApplication;
import com.metabroadcast.common.social.user.AccessTokenProcessor;
import com.metabroadcast.common.social.user.FixedAppIdUserRefBuilder;
import com.metabroadcast.common.social.user.TwitterOAuth1AccessTokenChecker;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;

@Configuration
@Import({AtlasPersistenceModule.class, ApplicationPersistenceModule.class, NotifierModule.class})
public class ApplicationWebModule {
    
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final SourceIdCodec sourceIdCodec = new SourceIdCodec(idCodec);
    private final JsonDeserializer<Id> idDeserializer = new IdDeserializer(idCodec);
    private final JsonDeserializer<DateTime> datetimeDeserializer = new JodaDateTimeSerializer();
    private final JsonDeserializer<SourceReadEntry> readsDeserializer = new SourceReadEntryDeserializer();
    private final JsonDeserializer<Publisher> publisherDeserializer = new PublisherDeserializer();
    private final JsonDeserializer<Role> roleDeserializer = new RoleDeserializer();
    
    @Autowired AtlasPersistenceModule persistence;
    @Autowired ApplicationPersistenceModule appPersistence;
    @Autowired NotifierModule notifier;
    
    private static final String APP_NAME = "atlas";
    
    @Value("${twitter.auth.consumerKey}") private String consumerKey;
    @Value("${twitter.auth.consumerSecret}") private String consumerSecret;
    
    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(DateTime.class, datetimeDeserializer)
            .registerTypeAdapter(Id.class, idDeserializer)
            .registerTypeAdapter(SourceReadEntry.class, readsDeserializer)
            .registerTypeAdapter(Publisher.class, publisherDeserializer)
            .registerTypeAdapter(Role.class, roleDeserializer)
            .setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES)
            .create();
    
    @Bean
    protected ModelReader gsonModelReader() {
        return new GsonModelReader(gson);
    }
    
    @Bean
    ResourceAnnotationIndex applicationAnnotationIndex() {
        return ResourceAnnotationIndex.builder(Resource.APPLICATION, Annotation.all()).build();
    }
    
    @Bean
    SelectionBuilder selectionBuilder() {
        return Selection.builder().withDefaultLimit(50).withMaxLimit(100);
    }
    

    
    public @Bean
    AuthController authController() {
        return new AuthController(new AuthProvidersQueryResultWriter(new AuthProvidersListWriter()),
                userFetcher(), idCodec);
    }
    
    @Bean
    public ApplicationsController applicationAdminController() {
        return new ApplicationsController(
                applicationQueryParser(),
                new ApplicationQueryExecutor(appPersistence.applicationStore()),
                new ApplicationQueryResultWriter(applicationListWriter()),
                gsonModelReader(),
                idCodec,
                sourceIdCodec,
                appPersistence.applicationStore(),
                userFetcher(),
                appPersistence.userStore());
    }
    
    public @Bean DefaultAnnotationHandlerMapping controllerMappings() {
        DefaultAnnotationHandlerMapping controllerClassNameHandlerMapping = new DefaultAnnotationHandlerMapping();
        Object[] interceptors = { getAuthenticationInterceptor() };
        controllerClassNameHandlerMapping.setInterceptors(interceptors);
        return controllerClassNameHandlerMapping;
    }
    
    @Bean OAuthInterceptor getAuthenticationInterceptor() {
        return OAuthInterceptor
                .builder()
                .withUserFetcher(userFetcher())
                .withIdCodec(idCodec)
                .withUrlsToProtect(ImmutableSet.of(
                        "/4.0/applications",
                        "/4.0/sources",
                        "/4.0/requests",
                        "/4.0/users",
                        "/4.0/auth/user"))
               .withUrlsNotNeedingCompleteProfile(ImmutableSet.of(
                "/4.0/auth/user",
                "/4.0/users/:uid"))
                .build();
    }
    
    @Bean 
    public SourcesController sourcesController() {
        return new SourcesController(sourcesQueryParser(), 
                soucesQueryExecutor(),
                new SourcesQueryResultWriter(new SourceWithIdWriter(sourceIdCodec, "source", "sources")),
                idCodec,
                sourceIdCodec,
                appPersistence.applicationStore(),
                userFetcher());
    }
    
    @Bean
    public SourceRequestsController sourceRequestsController() {
        IdGenerator idGenerator = new MongoSequentialIdGenerator(persistence.databasedMongo(), "sourceRequest");
        SourceRequestManager manager = new SourceRequestManager(appPersistence.sourceRequestStore(), 
                appPersistence.applicationStore(), 
                idGenerator,
                notifier.emailSender());
        return new SourceRequestsController(sourceRequestsQueryParser(),
                new SourceRequestQueryExecutor(appPersistence.sourceRequestStore()),
                new SourceRequestsQueryResultsWriter(new SourceRequestListWriter(sourceIdCodec, idCodec)),
                manager,
                idCodec,
                sourceIdCodec,
                userFetcher());
    }
    
    @Bean
    public UsersController usersController() {
        return new UsersController(usersQueryParser(),
                new UsersQueryExecutor(appPersistence.userStore()),
                new UsersQueryResultWriter(usersListWriter()),
                gsonModelReader(),
                idCodec,
                userFetcher(),
                appPersistence.userStore());
    }
    
    private StandardUserAwareQueryParser<Application> applicationQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(), userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()), selectionBuilder());

        return new StandardUserAwareQueryParser<Application>(Resource.APPLICATION,
                new QueryAttributeParser(ImmutableList.of(
                    QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec)),
                    QueryAtomParser.valueOf(Attributes.SOURCE_READS, AttributeCoercers.sourceIdCoercer(sourceIdCodec)),
                    QueryAtomParser.valueOf(Attributes.SOURCE_WRITES, AttributeCoercers.sourceIdCoercer(sourceIdCodec))
                    )),
                idCodec, contextParser);
    }
    
    @Bean
    protected EntityListWriter<Application> applicationListWriter() {
        return new ApplicationListWriter(idCodec, sourceIdCodec);
    }
    
    private StandardUserAwareQueryParser<User> usersQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(), userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()), selectionBuilder());

        return new StandardUserAwareQueryParser<User>(Resource.USER,
                new QueryAttributeParser(ImmutableList.of(
                    QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec))
                )),
                idCodec, contextParser);
    }
    
    @Bean
    protected EntityListWriter<User> usersListWriter() {
        return new UsersListWriter(idCodec, sourceIdCodec);
    }
    
    public @Bean
    ApplicationSourcesFetcher configFetcher() {
         return new ApiKeySourcesFetcher(appPersistence.applicationStore());
    }
    
    public @Bean
    UserFetcher userFetcher() {
        CachingAccessTokenChecker cachingAccessTokenChecker = new CachingAccessTokenChecker(accessTokenChecker());
        return new OAuthTokenUserFetcher(appPersistence.credentialsStore(), cachingAccessTokenChecker, appPersistence.userStore());
    }
    
    private StandardUserAwareQueryParser<Publisher> sourcesQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(), userFetcher(), 
                new IndexAnnotationsExtractor(applicationAnnotationIndex()), selectionBuilder());

        return new StandardUserAwareQueryParser<Publisher>(Resource.SOURCE,
                new QueryAttributeParser(ImmutableList.of(
                    QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec))
                )),
                idCodec, contextParser);
    }
    
    @Bean
    protected UserAwareQueryExecutor<Publisher> soucesQueryExecutor() {
        return new SourcesQueryExecutor(sourceIdCodec);
    }
    
    private StandardUserAwareQueryParser<SourceRequest> sourceRequestsQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(), userFetcher(), 
                new IndexAnnotationsExtractor(applicationAnnotationIndex()), selectionBuilder());

        return new StandardUserAwareQueryParser<SourceRequest>(Resource.SOURCE_REQUEST,
                new QueryAttributeParser(ImmutableList.of(
                        QueryAtomParser.valueOf(Attributes.SOURCE_REQUEST_SOURCE,
                                AttributeCoercers.sourceIdCoercer(sourceIdCodec))
                    )),
                idCodec, contextParser);
    }

    public @Bean TwitterAuthController twitterAuthController() {
        return new TwitterAuthController(new TwitterApplication(consumerKey, consumerSecret), 
                accessTokenProcessor(),
                appPersistence.userStore(), 
                new NewUserSupplier(new MongoSequentialIdGenerator(persistence.databasedMongo(), "users")),
                appPersistence.tokenStore(),
                new OAuthRequestQueryResultWriter(new OAuthRequestListWriter()),
                new OAuthResultQueryResultWriter(new OAuthResultListWriter())
                );
    }


    public @Bean FixedAppIdUserRefBuilder userRefBuilder() {
        return new FixedAppIdUserRefBuilder(APP_NAME);
    }
    
    public @Bean AccessTokenChecker accessTokenChecker() {
        return new TwitterOAuth1AccessTokenChecker(userRefBuilder() , consumerKey, consumerSecret);
    }
    
    public @Bean AccessTokenProcessor accessTokenProcessor() {
        return new AccessTokenProcessor(accessTokenChecker(), appPersistence.credentialsStore());
    }

}
