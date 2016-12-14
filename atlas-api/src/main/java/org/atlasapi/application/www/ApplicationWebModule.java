package org.atlasapi.application.www;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.LicenseModule;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationPersistenceModule;
import org.atlasapi.application.ApplicationQueryExecutor;
import org.atlasapi.application.ApplicationQueryExecutorMultipleAccounts;
import org.atlasapi.application.ApplicationsController;
import org.atlasapi.application.SourceLicense;
import org.atlasapi.application.SourceLicenseController;
import org.atlasapi.application.SourceLicenseQueryExecutor;
import org.atlasapi.application.SourceLicenseQueryExecutorMultipleAccounts;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceRequest;
import org.atlasapi.application.SourceRequestManager;
import org.atlasapi.application.SourceRequestQueryExecutor;
import org.atlasapi.application.SourceRequestQueryExecutorMultipleAccounts;
import org.atlasapi.application.SourceRequestsController;
import org.atlasapi.application.SourcesController;
import org.atlasapi.application.SourcesQueryExecutor;
import org.atlasapi.application.SourcesQueryExecutorMultipleAccounts;
import org.atlasapi.application.auth.ApiKeyApplicationFetcher;
import org.atlasapi.application.auth.ApplicationFetcher;
import org.atlasapi.application.auth.AuthProvidersListWriter;
import org.atlasapi.application.auth.AuthProvidersQueryResultWriter;
import org.atlasapi.application.auth.NoAuthUserFetcher;
import org.atlasapi.application.auth.OAuthInterceptor;
import org.atlasapi.application.auth.OAuthRequestListWriter;
import org.atlasapi.application.auth.OAuthRequestQueryResultWriter;
import org.atlasapi.application.auth.OAuthResultListWriter;
import org.atlasapi.application.auth.OAuthResultQueryResultWriter;
import org.atlasapi.application.auth.OAuthTokenUserFetcher;
import org.atlasapi.application.auth.UserFetcher;
import org.atlasapi.application.auth.github.GitHubAccessTokenChecker;
import org.atlasapi.application.auth.github.GitHubAuthClient;
import org.atlasapi.application.auth.github.GitHubAuthController;
import org.atlasapi.application.auth.google.GoogleAccessTokenChecker;
import org.atlasapi.application.auth.google.GoogleAuthClient;
import org.atlasapi.application.auth.google.GoogleAuthController;
import org.atlasapi.application.auth.twitter.TwitterAuthController;
import org.atlasapi.application.auth.www.AuthController;
import org.atlasapi.application.model.deserialize.IdDeserializer;
import org.atlasapi.application.model.deserialize.OptionalDeserializer;
import org.atlasapi.application.model.deserialize.PublisherDeserializer;
import org.atlasapi.application.model.deserialize.RoleDeserializer;
import org.atlasapi.application.model.deserialize.SourceReadEntryDeserializer;
import org.atlasapi.application.sources.SourceIdCodec;
import org.atlasapi.application.users.EndUserLicenseController;
import org.atlasapi.application.users.NewUserSupplier;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UsersController;
import org.atlasapi.application.users.UsersQueryExecutor;
import org.atlasapi.application.users.UsersQueryExecutorMultipleAccounts;
import org.atlasapi.application.writers.ApplicationListWriter;
import org.atlasapi.application.writers.ApplicationQueryResultWriter;
import org.atlasapi.application.writers.ApplicationQueryResultWriterMultipleAccounts;
import org.atlasapi.application.writers.EndUserLicenseListWriter;
import org.atlasapi.application.writers.EndUserLicenseQueryResultWriter;
import org.atlasapi.application.writers.SourceLicenseQueryResultWriter;
import org.atlasapi.application.writers.SourceLicenseQueryResultWriterMultipleAccounts;
import org.atlasapi.application.writers.SourceLicenseWithIdWriter;
import org.atlasapi.application.writers.SourceRequestListWriter;
import org.atlasapi.application.writers.SourceRequestsQueryResultsWriter;
import org.atlasapi.application.writers.SourceWithIdWriter;
import org.atlasapi.application.writers.SourcesQueryResultWriter;
import org.atlasapi.application.writers.SourcesQueryResultWriterMultipleAccounts;
import org.atlasapi.application.writers.SourcesRequestsQueryResultsWriter;
import org.atlasapi.application.writers.UsersListWriter;
import org.atlasapi.application.writers.UsersQueryResultWriter;
import org.atlasapi.application.writers.UsersQueryResultWriterMultipleAccounts;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.input.GsonModelReader;
import org.atlasapi.input.ModelReader;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.writers.RequestWriter;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.query.annotation.ResourceAnnotationIndex;
import org.atlasapi.query.common.IndexAnnotationsExtractor;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.attributes.QueryAtomParser;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.coercers.SourceIdStringCoercer;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParser;
import org.atlasapi.query.common.useraware.StandardUserAwareQueryParserNoAuth;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryExecutor;
import org.atlasapi.query.common.useraware.UserAwareQueryContextParser;
import org.atlasapi.query.common.useraware.UserAwareQueryContextParserNoAuth;
import org.atlasapi.query.common.useraware.UserAwareQueryExecutor;
import org.atlasapi.users.videosource.VideoSourceChannelResultsListWriter;
import org.atlasapi.users.videosource.VideoSourceChannelResultsQueryResultWriter;
import org.atlasapi.users.videosource.VideoSourceController;
import org.atlasapi.users.videosource.VideoSourceOAuthProvidersQueryResultWriter;
import org.atlasapi.users.videosource.VideoSourceOauthProvidersListWriter;
import org.atlasapi.users.videosource.remote.RemoteSourceUpdaterClient;
import org.atlasapi.users.videosource.youtube.YouTubeLinkedServiceController;

import com.metabroadcast.common.http.HttpClients;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.query.Selection.SelectionBuilder;
import com.metabroadcast.common.social.auth.facebook.AccessTokenChecker;
import com.metabroadcast.common.social.auth.facebook.CachingAccessTokenChecker;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;
import com.metabroadcast.common.social.twitter.TwitterApplication;
import com.metabroadcast.common.social.user.AccessTokenProcessor;
import com.metabroadcast.common.social.user.FixedAppIdUserRefBuilder;
import com.metabroadcast.common.social.user.TwitterOAuth1AccessTokenChecker;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.webapp.serializers.JodaDateTimeSerializer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.reflect.TypeToken;
import org.elasticsearch.common.collect.Maps;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.mvc.annotation.DefaultAnnotationHandlerMapping;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

@Configuration
@Import({
        AtlasPersistenceModule.class,
        ApplicationPersistenceModule.class,
        LicenseModule.class
})
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

    @Autowired @Qualifier("licenseWriter") EntityWriter<Object> licenseWriter;

    private static final String APP_NAME = "atlas";

    @Value("${twitter.auth.consumerKey}") private String twitterConsumerKey;
    @Value("${twitter.auth.consumerSecret}") private String twitterConsumerSecret;

    @Value("${github.auth.consumerKey}") private String githubConsumerKey;
    @Value("${github.auth.consumerSecret}") private String githubConsumerSecret;

    @Value("${google.auth.consumerKey}") private String googleConsumerKey;
    @Value("${google.auth.consumerSecret}") private String googleConsumerSecret;

    @Value("${youtube.clientId}") private String youTubeClientId;
    @Value("${youtube.clientSecret}") private String youTubeClientSecret;

    @Value("${youtube.handling.service}") private String handlingService;

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(DateTime.class, datetimeDeserializer)
            .registerTypeAdapter(Id.class, idDeserializer)
            .registerTypeAdapter(SourceReadEntry.class, readsDeserializer)
            .registerTypeAdapter(Publisher.class, publisherDeserializer)
            .registerTypeAdapter(Role.class, roleDeserializer)
            .registerTypeAdapter(new TypeToken<Optional<DateTime>>() {

            }.getType(), new OptionalDeserializer())
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

    @Bean
    EntityWriter<HttpServletRequest> requestWriter() {
        return new RequestWriter();

    }

    public
    @Bean
    AuthController authController() {
        return new AuthController(
                new AuthProvidersQueryResultWriter(
                        new AuthProvidersListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                userFetcher(),
                noAuthUserFetcher(),
                idCodec
        );
    }

    @Bean
    public ApplicationsController applicationAdminController() {
        return new ApplicationsController(
                applicationQueryParser(),
                applicationQueryParserNoAuth(),
                new ApplicationQueryExecutor(appPersistence.applicationStore()),
                new ApplicationQueryExecutorMultipleAccounts(appPersistence.applicationStore()),
                new ApplicationQueryResultWriter(applicationListWriter()),
                new ApplicationQueryResultWriterMultipleAccounts(applicationListWriter()),
                gsonModelReader(),
                idCodec,
                sourceIdCodec,
                appPersistence.applicationStore(),
                userFetcher(),
                noAuthUserFetcher(),
                appPersistence.userStore()
        );
    }

    public
    @Bean
    DefaultAnnotationHandlerMapping controllerMappings() {
        DefaultAnnotationHandlerMapping controllerClassNameHandlerMapping = new DefaultAnnotationHandlerMapping();
        Object[] interceptors = { getAuthenticationInterceptor() };
        controllerClassNameHandlerMapping.setInterceptors(interceptors);
        return controllerClassNameHandlerMapping;
    }

    @Bean
    OAuthInterceptor getAuthenticationInterceptor() {
        return OAuthInterceptor
                .builder()
                .withUserFetcher(userFetcher())
                .withIdCodec(idCodec)
                .withUrlsToProtect(ImmutableSet.of(
                        "/4/applications",
                        "/4/sources",
                        "/4/requests",
                        "/4/users",
                        "/4/auth/user",
                        "/4/videosource"
                ))
                .withUrlsNotNeedingCompleteProfile(ImmutableSet.of(
                        "/4/auth/user",
                        "/4/users/:uid",
                        "/4/eula",
                        "/4/users/:uid/eula/accept"
                ))
                .withExemptions(ImmutableSet.of(
                        "/4/videosource/youtube/token.json"
                ))
                .build();
    }

    @Bean
    public SourcesController sourcesController() {
        return new SourcesController(
                sourcesQueryParser(),
                sourcesQueryParserNoAuth(),
                soucesQueryExecutor(),
                new SourcesQueryResultWriter(getSourcesWriter()),
                soucesQueryExecutorMultipleAccounts(),
                new SourcesQueryResultWriterMultipleAccounts(getSourcesWriter()),
                idCodec,
                sourceIdCodec,
                appPersistence.applicationStore(),
                userFetcher(),
                noAuthUserFetcher()
        );
    }

    private SourceWithIdWriter getSourcesWriter() {
        return new SourceWithIdWriter(
                sourceIdCodec,
                "source",
                "sources"
        );
    }

    @Bean
    public SourceRequestsController sourceRequestsController() {
        IdGenerator idGenerator = new MongoSequentialIdGenerator(
                persistence.databasedWriteMongo(),
                "sourceRequest"
        );
        SourceRequestManager manager = new SourceRequestManager(
                appPersistence.sourceRequestStore(),
                appPersistence.applicationStore(),
                idGenerator,
                new SystemClock()
        );
        return new SourceRequestsController(
                sourceRequestsQueryParser(),
                sourceRequestParserNoAuth(),
                noAuthUserFetcher(),
                new SourceRequestQueryExecutor(appPersistence.sourceRequestStore()),
                new SourceRequestsQueryResultsWriter(new SourceRequestListWriter(
                        sourceIdCodec,
                        idCodec
                )),
                new SourceRequestQueryExecutorMultipleAccounts(appPersistence.sourceRequestStore()),
                new SourcesRequestsQueryResultsWriter(new SourceRequestListWriter(
                        sourceIdCodec,
                        idCodec
                )),
                manager,
                idCodec,
                sourceIdCodec,
                userFetcher()
        );
    }

    @Bean
    public UsersController usersController() {
        return new UsersController(
                usersQueryParser(),
                usersQueryParserNoAuth(),
                new UsersQueryExecutor(appPersistence.userStore()),
                new UsersQueryResultWriter(usersListWriter()),
                new UsersQueryExecutorMultipleAccounts(appPersistence.userStore()),
                new UsersQueryResultWriterMultipleAccounts(usersListWriter()),
                gsonModelReader(),
                idCodec,
                userFetcher(),
                noAuthUserFetcher(),
                appPersistence.userStore(),
                getUsersIdGenerator(),
                new SystemClock()
        );
    }

    private StandardUserAwareQueryParser<Application> applicationQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(),
                userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParser<Application>(Resource.APPLICATION,
                getAttributeParser(),
                idCodec, contextParser
        );
    }

    private StandardUserAwareQueryParserNoAuth<Application> applicationQueryParserNoAuth() {
        UserAwareQueryContextParserNoAuth contextParser = new UserAwareQueryContextParserNoAuth(configFetcher(),
                noAuthUserFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParserNoAuth<Application>(Resource.APPLICATION,
                getAttributeParser(),
                idCodec, contextParser
        );
    }

    private QueryAttributeParser getAttributeParser() {
        return QueryAttributeParser.create(ImmutableList.of(
                QueryAtomParser.create(
                        Attributes.ID,
                        IdCoercer.create(idCodec)
                ),
                QueryAtomParser.create(
                        Attributes.SOURCE_READS,
                        SourceIdStringCoercer.create(sourceIdCodec)
                ),
                QueryAtomParser.create(
                        Attributes.SOURCE_WRITES,
                        SourceIdStringCoercer.create(sourceIdCodec)
                )
        ));
    }

    @Bean
    protected EntityListWriter<Application> applicationListWriter() {
        return new ApplicationListWriter(idCodec, sourceIdCodec);
    }

    private StandardUserAwareQueryParser<User> usersQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(),
                userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParser<User>(Resource.USER,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    private StandardUserAwareQueryParserNoAuth<User> usersQueryParserNoAuth() {
        UserAwareQueryContextParserNoAuth contextParser = new UserAwareQueryContextParserNoAuth(configFetcher(),
                noAuthUserFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParserNoAuth<User>(Resource.USER,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    @Bean
    protected EntityListWriter<User> usersListWriter() {
        return new UsersListWriter(idCodec, sourceIdCodec);
    }

    public
    @Bean
    ApplicationFetcher configFetcher() {
        return new ApiKeyApplicationFetcher(appPersistence.applicationsClient());
    }

    public
    @Bean
    UserFetcher userFetcher() {
        Map<UserNamespace, AccessTokenChecker> checkers = Maps.newHashMap();
        checkers.put(
                UserNamespace.TWITTER,
                new CachingAccessTokenChecker(twitterAccessTokenChecker())
        );
        checkers.put(
                UserNamespace.GITHUB,
                new CachingAccessTokenChecker(gitHubAccessTokenChecker())
        );
        checkers.put(
                UserNamespace.GOOGLE,
                new CachingAccessTokenChecker(googleAccessTokenChecker())
        );
        return new OAuthTokenUserFetcher(
                appPersistence.credentialsStore(),
                checkers,
                appPersistence.userStore()
        );
    }

    @Bean
    public NoAuthUserFetcher noAuthUserFetcher() {
        return new NoAuthUserFetcher(appPersistence.userStore());
    }

    private StandardUserAwareQueryParser<Publisher> sourcesQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(),
                userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParser<Publisher>(Resource.SOURCE,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    private StandardUserAwareQueryParserNoAuth<Publisher> sourcesQueryParserNoAuth() {
        UserAwareQueryContextParserNoAuth contextParser = new UserAwareQueryContextParserNoAuth(configFetcher(),
                noAuthUserFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParserNoAuth<Publisher>(Resource.SOURCE,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    @Bean
    protected UserAwareQueryExecutor<Publisher> soucesQueryExecutor() {
        return new SourcesQueryExecutor(sourceIdCodec);
    }

    @Bean
    protected UserAccountsAwareQueryExecutor<Publisher> soucesQueryExecutorMultipleAccounts() {
        return new SourcesQueryExecutorMultipleAccounts(sourceIdCodec);
    }

    private StandardUserAwareQueryParser<SourceRequest> sourceRequestsQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(),
                userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );

        return new StandardUserAwareQueryParser<SourceRequest>(Resource.SOURCE_REQUEST,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.SOURCE_REQUEST_SOURCE,
                                SourceIdStringCoercer.create(
                                                                sourceIdCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    private StandardUserAwareQueryParserNoAuth<SourceRequest> sourceRequestParserNoAuth() {
        UserAwareQueryContextParserNoAuth contextParser = new UserAwareQueryContextParserNoAuth(configFetcher(),
                noAuthUserFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );
        return new StandardUserAwareQueryParserNoAuth<SourceRequest>(Resource.SOURCE_REQUEST,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.SOURCE_REQUEST_SOURCE,
                                SourceIdStringCoercer.create(
                                                                sourceIdCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    private NewUserSupplier newUserSupplier() {
        return new NewUserSupplier(getUsersIdGenerator());
    }

    private MongoSequentialIdGenerator getUsersIdGenerator() {
        return new MongoSequentialIdGenerator(
                persistence.databasedWriteMongo(),
                "users"
        );
    }

    public
    @Bean
    TwitterAuthController twitterAuthController() {
        return new TwitterAuthController(
                new TwitterApplication(twitterConsumerKey, twitterConsumerSecret),
                new AccessTokenProcessor(
                        twitterAccessTokenChecker(),
                        appPersistence.credentialsStore()
                ),
                appPersistence.userStore(),
                newUserSupplier(),
                appPersistence.tokenStore(),
                new OAuthRequestQueryResultWriter(
                        new OAuthRequestListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                new OAuthResultQueryResultWriter(
                        new OAuthResultListWriter(),
                        licenseWriter,
                        requestWriter()
                )
        );
    }

    public
    @Bean
    GitHubAuthController gitHubAuthController() {
        return new GitHubAuthController(
                gitHubClient(),
                new AccessTokenProcessor(
                        gitHubAccessTokenChecker(),
                        appPersistence.credentialsStore()
                ),
                appPersistence.userStore(),
                newUserSupplier(),
                appPersistence.tokenStore(),
                new OAuthRequestQueryResultWriter(
                        new OAuthRequestListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                new OAuthResultQueryResultWriter(
                        new OAuthResultListWriter(),
                        licenseWriter,
                        requestWriter()
                )
        );
    }

    public
    @Bean
    GoogleAuthController googleAuthController() {
        return new GoogleAuthController(
                googleClient(),
                appPersistence.userStore(),
                newUserSupplier(),
                new OAuthRequestQueryResultWriter(
                        new OAuthRequestListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                new OAuthResultQueryResultWriter(
                        new OAuthResultListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                new AccessTokenProcessor(
                        googleAccessTokenChecker(),
                        appPersistence.credentialsStore()
                ),
                appPersistence.tokenStore()
        );
    }

    public
    @Bean
    FixedAppIdUserRefBuilder userRefBuilder() {
        return new FixedAppIdUserRefBuilder(APP_NAME);
    }

    private GitHubAuthClient gitHubClient() {
        return new GitHubAuthClient(githubConsumerKey, githubConsumerSecret);
    }

    private GoogleAuthClient googleClient() {
        return new GoogleAuthClient(googleConsumerKey, googleConsumerSecret);
    }

    public
    @Bean
    AccessTokenChecker twitterAccessTokenChecker() {
        return new TwitterOAuth1AccessTokenChecker(
                userRefBuilder(),
                twitterConsumerKey,
                twitterConsumerSecret
        );
    }

    public
    @Bean
    AccessTokenChecker gitHubAccessTokenChecker() {
        return new GitHubAccessTokenChecker(userRefBuilder(), gitHubClient());
    }

    public
    @Bean
    AccessTokenChecker googleAccessTokenChecker() {
        return new GoogleAccessTokenChecker(userRefBuilder(), googleClient());
    }

    public
    @Bean
    VideoSourceController linkedServiceController() {
        return new VideoSourceController(
                new VideoSourceOAuthProvidersQueryResultWriter(
                        new VideoSourceOauthProvidersListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                userFetcher()
        );
    }

    VideoSourceChannelResultsQueryResultWriter videoSourceChannelResultsQueryResultWriter() {
        return new VideoSourceChannelResultsQueryResultWriter(
                new VideoSourceChannelResultsListWriter(),
                licenseWriter,
                requestWriter()
        );
    }

    @Bean
    public SimpleHttpClient httpClient() {
        return HttpClients.webserviceClient();
    }

    public
    @Bean
    YouTubeLinkedServiceController youTubeLinkedServiceController() {

        RemoteSourceUpdaterClient sourceUpdaterClient = new RemoteSourceUpdaterClient(
                gson,
                handlingService,
                httpClient()
        );
        return new YouTubeLinkedServiceController(
                youTubeClientId,
                youTubeClientSecret,
                new OAuthRequestQueryResultWriter(
                        new OAuthRequestListWriter(),
                        licenseWriter,
                        requestWriter()
                ),
                userFetcher(),
                idCodec,
                sourceIdCodec,
                appPersistence.linkedOauthTokenUserStore(),
                sourceUpdaterClient,
                videoSourceChannelResultsQueryResultWriter()
        );
    }

    private StandardUserAwareQueryParser<SourceLicense> sourceLicenseQueryParser() {
        UserAwareQueryContextParser contextParser = new UserAwareQueryContextParser(configFetcher(),
                userFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );
        return new StandardUserAwareQueryParser<SourceLicense>(Resource.SOURCE_LICENSE,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    private StandardUserAwareQueryParserNoAuth<SourceLicense> sourceLicenseQueryParserNoAuth() {
        UserAwareQueryContextParserNoAuth contextParser = new UserAwareQueryContextParserNoAuth(configFetcher(),
                noAuthUserFetcher(),
                new IndexAnnotationsExtractor(applicationAnnotationIndex()),
                selectionBuilder()
        );
        return new StandardUserAwareQueryParserNoAuth<SourceLicense>(Resource.SOURCE_LICENSE,
                QueryAttributeParser.create(ImmutableList.of(
                        QueryAtomParser.create(
                                Attributes.ID,
                                IdCoercer.create(idCodec)
                        )
                )),
                idCodec, contextParser
        );
    }

    @Bean
    protected UserAwareQueryExecutor<SourceLicense> souceLicenseQueryExecutor() {
        return new SourceLicenseQueryExecutor(sourceIdCodec, appPersistence.sourceLicenseStore());
    }
    @Bean
    protected UserAccountsAwareQueryExecutor
            <SourceLicense> souceLicenseQueryExecutorMultipleAccounts() {
        return new SourceLicenseQueryExecutorMultipleAccounts(sourceIdCodec, appPersistence.sourceLicenseStore());
    }


    public
    @Bean
    SourceLicenseController sourceLicenseController() {
        return new SourceLicenseController(
                sourceLicenseQueryParser(),
                sourceLicenseQueryParserNoAuth(),
                souceLicenseQueryExecutor(),
                new SourceLicenseQueryResultWriter(new SourceLicenseWithIdWriter(sourceIdCodec)),
                souceLicenseQueryExecutorMultipleAccounts(),
                new SourceLicenseQueryResultWriterMultipleAccounts(new SourceLicenseWithIdWriter(sourceIdCodec)),
                gsonModelReader(),
                userFetcher(),
                noAuthUserFetcher(),
                appPersistence.sourceLicenseStore()
        );
    }

    @Bean
    public EndUserLicenseController endUserLicenseController() {
        EndUserLicenseListWriter endUserLicenseListWriter = new EndUserLicenseListWriter();

        return new EndUserLicenseController(new EndUserLicenseQueryResultWriter(
                endUserLicenseListWriter,
                licenseWriter,
                requestWriter()
        ),
                gsonModelReader(), appPersistence.endUserLicenseStore(), userFetcher(), noAuthUserFetcher()
        );
    }

}
