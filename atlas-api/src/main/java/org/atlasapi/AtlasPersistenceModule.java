package org.atlasapi;

import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.CassandraEquivalentContentStore;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.content.EsContentTranslator;
import org.atlasapi.content.v2.CqlContentStore;
import org.atlasapi.content.v2.NonValidatingCqlWriter;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;
import org.atlasapi.hashing.HashGenerator;
import org.atlasapi.hashing.content.ContentHashGenerator;
import org.atlasapi.media.channel.CachingChannelGroupStore;
import org.atlasapi.media.channel.CachingChannelStore;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.channel.ServiceChannelStore;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.messaging.EquivalentContentUpdatedMessage;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.MessagingModule;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.neo4j.Neo4jModule;
import org.atlasapi.neo4j.service.Neo4jContentStore;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationStore;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoPlayerStore;
import org.atlasapi.persistence.content.mongo.MongoServiceStore;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.organisation.MongoOrganisationStore;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.event.MongoEventStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.CachingPlayerResolver;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.CachingServiceResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.schedule.ScheduleWriter;
import org.atlasapi.segment.SegmentStore;
import org.atlasapi.system.MetricsModule;
import org.atlasapi.system.legacy.ContentListerResourceListerAdapter;
import org.atlasapi.system.legacy.LegacyChannelGroupResolver;
import org.atlasapi.system.legacy.LegacyChannelGroupTransformer;
import org.atlasapi.system.legacy.LegacyChannelResolver;
import org.atlasapi.system.legacy.LegacyChannelTransformer;
import org.atlasapi.system.legacy.LegacyContentLister;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.system.legacy.LegacyContentTransformer;
import org.atlasapi.system.legacy.LegacyEventResolver;
import org.atlasapi.system.legacy.LegacyLookupResolvingContentLister;
import org.atlasapi.system.legacy.LegacyOrganisationResolver;
import org.atlasapi.system.legacy.LegacyOrganisationTransformer;
import org.atlasapi.system.legacy.LegacyScheduleResolver;
import org.atlasapi.system.legacy.LegacySegmentMigrator;
import org.atlasapi.system.legacy.LegacyTopicLister;
import org.atlasapi.system.legacy.LegacyTopicResolver;
import org.atlasapi.system.legacy.PaTagMap;
import org.atlasapi.topic.EsPopularTopicIndex;
import org.atlasapi.topic.EsTopicIndex;
import org.atlasapi.topic.TopicStore;
import org.atlasapi.util.CassandraSecondaryIndex;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoConnectionPoolProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenders;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.extras.codecs.joda.InstantCodec;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.json.JacksonJsonCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Import({ KafkaMessagingModule.class })
public class AtlasPersistenceModule {

    private static final String MONGO_COLLECTION_LOOKUP = "lookup";
    private static final String MONGO_COLLECTION_TOPICS = "topics";
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private static final PersistenceAuditLog persistenceAuditLog = new NoLoggingPersistenceAuditLog();

    private final String mongoWriteHost = Configurer.get("mongo.write.host").get();
    private final Integer mongoWritePort = Configurer.get("mongo.write.port").toInt();
    private final String mongoWriteDbName = Configurer.get("mongo.write.name").get();

    private final String mongoReadHost = Configurer.get("mongo.read.host").get();
    private final Integer mongoReadPort = Configurer.get("mongo.read.port").toInt();
    private final String mongoReadDbName = Configurer.get("mongo.read.name").get();

    private final String cassandraCluster = Configurer.get("cassandra.cluster").get();
    private final String cassandraKeyspace = Configurer.get("cassandra.keyspace").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final String cassandraPort = Configurer.get("cassandra.port").get();
    private final String cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout")
            .get();
    private final String cassandraClientThreads = Configurer.get("cassandra.clientThreads").get();
    private final Integer cassandraConnectionsPerHostLocal = Configurer.get(
            "cassandra.connectionsPerHost.local").toInt();
    private final Integer cassandraConnectionsPerHostRemote = Configurer.get(
            "cassandra.connectionsPerHost.remote").toInt();
    private final Integer cassandraDatastaxConnectionTimeout = Configurer.get(
            "cassandra.datastax.timeouts.connections").toInt();
    private final Integer cassandraDatastaxReadTimeout = Configurer.get(
            "cassandra.datastax.timeouts.read").toInt();

    private final String esSeeds = Configurer.get("elasticsearch.seeds").get();
    private final int port = Ints.saturatedCast(Configurer.get("elasticsearch.port").toLong());
    private final boolean ssl = Configurer.get("elasticsearch.ssl").toBoolean();
    private final String esCluster = Configurer.get("elasticsearch.cluster").get();
    private final String esIndex = Configurer.get("elasticsearch.index").get();
    private final String esRequestTimeout = Configurer.get("elasticsearch.requestTimeout").get();
    private final Parameter processingConfig = Configurer.get("processing.config");

    private final String neo4jHost = Configurer.get("neo4j.host").get();
    private final Integer neo4jPort = Configurer.get("neo4j.port").toInt();
    private final Integer neo4jMaxIdleSessions = Configurer.get("neo4j.maxIdleSessions").toInt();

    private String equivalentContentChanges = Configurer
            .get("messaging.destination.equivalent.content.changes").get();
    private String equivalentContentGraphChanges = Configurer
            .get("messaging.destination.equivalent.content.graph.changes").get();

    private @Autowired MessagingModule messaging;
    private @Autowired MetricsModule metricsModule;

    @PostConstruct
    public void init() {
        persistenceModule().startAsync().awaitRunning();

        // This is required to initialise the BackgroundComputingValue in the CachingChannelStore
        // otherwise we will get NPEs
        channelStore().start();
    }

    @PreDestroy
    public void tearDown() {
        channelStore().shutdown();
    }

    @Bean
    public CassandraPersistenceModule persistenceModule() {
        Iterable<String> seeds = Splitter.on(",").split(cassandraSeeds);
        ConfiguredAstyanaxContext contextSupplier = new ConfiguredAstyanaxContext(cassandraCluster,
                cassandraKeyspace,
                seeds,
                Integer.parseInt(cassandraPort),
                Integer.parseInt(cassandraClientThreads),
                Integer.parseInt(cassandraConnectionTimeout),
                metricsModule.metrics()
        );
        AstyanaxContext<Keyspace> context = contextSupplier.get();
        context.start();

        DatastaxCassandraService cassandraService = DatastaxCassandraService.builder()
                .withNodes(seeds)
                .withConnectionsPerHostLocal(cassandraConnectionsPerHostLocal)
                .withConnectionsPerHostRemote(cassandraConnectionsPerHostRemote)
                .withCodecRegistry(new CodecRegistry()
                        .register(InstantCodec.instance)
                        .register(LocalDateCodec.instance)
                        .register(new JacksonJsonCodec<>(
                                org.atlasapi.content.v2.model.Clip.Wrapper.class,
                                MAPPER
                        ))
                        .register(new JacksonJsonCodec<>(
                                org.atlasapi.content.v2.model.Encoding.Wrapper.class,
                                MAPPER
                        )))
                .withConnectTimeoutMillis(cassandraDatastaxConnectionTimeout)
                .withReadTimeoutMillis(cassandraDatastaxReadTimeout)
                .build();

        cassandraService.startAsync().awaitRunning();
        return new CassandraPersistenceModule(
                messaging.messageSenderFactory(),
                context,
                cassandraService,
                cassandraKeyspace,
                idGeneratorBuilder(),
                ContentHashGenerator.create(
                        HashGenerator.create(),
                        "persistence.util.",
                        metricsModule.metrics()
                ),
                eventV2 -> UUID.randomUUID().toString(),
                seeds,
                metricsModule.metrics()
        );
    }

    @Bean
    public ContentStore contentStore() {
        return persistenceModule().contentStore();
    }

    @Bean
    public CqlContentStore cqlContentStore() {
        return persistenceModule().cqlContentStore();
    }

    @Bean
    public NonValidatingCqlWriter forceCqlContentWriter() {
        return persistenceModule().forceCqlContentWriter();
    }

    public ContentStore nullMessageSendingContentStore() {
        return persistenceModule().nullMessageSendingContentStore();
    }

    public EquivalenceGraphStore nullMessageSendingGraphStore() {
        return persistenceModule().nullMessageSendingGraphStore();
    }

    @Bean
    public TopicStore topicStore() {
        return persistenceModule().topicStore();
    }

    @Bean
    public ScheduleStore scheduleStore() {
        return persistenceModule().scheduleStore();
    }

    @Bean
    public SegmentStore segmentStore() {
        return persistenceModule().segmentStore();
    }

    @Bean
    public EventWriter eventWriter() {
        return persistenceModule().eventStore();
    }
    @Bean
    public EventResolver eventResolver() {
        return persistenceModule().eventStore();
    }

    @Bean
    public OrganisationStore organisationStore() {
        return persistenceModule().organisationStore();
    }

    @Bean
    public OrganisationStore idSettingOrganisationStore() {
        return persistenceModule().idSettingOrganisationStore();
    }

    @Bean
    public EquivalenceGraphStore getContentEquivalenceGraphStore() {
        return persistenceModule().contentEquivalenceGraphStore();
    }

    @Bean
    public EquivalenceGraphStore nullMessageSendingEquivalenceGraphStore() {
        return persistenceModule().nullMessageSendingEquivalenceGraphStore();
    }

    @Bean
    public EquivalentContentStore getEquivalentContentStore() {
        return new CassandraEquivalentContentStore(
                persistenceModule().contentStore(),
                legacyContentResolver(),
                persistenceModule().contentEquivalenceGraphStore(),
                persistenceModule().sender(
                        equivalentContentChanges,
                        EquivalentContentUpdatedMessage.class
                ),
                persistenceModule().sender(
                        equivalentContentGraphChanges,
                        EquivalenceGraphUpdateMessage.class
                ),
                persistenceModule().getSession(),
                persistenceModule().getReadConsistencyLevel(),
                persistenceModule().getWriteConsistencyLevel()
        );
    }

    @Bean
    public EquivalentContentStore nullMessageSendingEquivalentContentStore() {
        return new CassandraEquivalentContentStore(
                persistenceModule().contentStore(),
                legacyContentResolver(),
                persistenceModule().contentEquivalenceGraphStore(),
                persistenceModule().nullMessageSender(EquivalentContentUpdatedMessage.class),
                persistenceModule().nullMessageSender(EquivalenceGraphUpdateMessage.class),
                persistenceModule().getSession(),
                persistenceModule().getReadConsistencyLevel(),
                persistenceModule().getWriteConsistencyLevel()
        );
    }

    @Bean
    public EquivalentScheduleStore getEquivalentScheduleStore() {
        return persistenceModule().equivalentScheduleStore();
    }

    @Bean
    public ElasticSearchContentIndexModule esContentIndexModule() {
        ElasticSearchContentIndexModule module =
                new ElasticSearchContentIndexModule(
                        esSeeds,
                        port,
                        ssl,
                        esCluster,
                        esIndex,
                        Long.parseLong(esRequestTimeout),
                        persistenceModule().contentStore(),
                        metricsModule.metrics(),
                        channelGroupResolver(),
                        new CassandraSecondaryIndex(
                                persistenceModule().getSession(),
                                CassandraEquivalentContentStore.EQUIVALENT_CONTENT_INDEX,
                                persistenceModule().getReadConsistencyLevel()
                        )
                );
        module.init();
        return module;
    }

    @Bean
    @Primary
    public DatabasedMongo databasedReadMongo() {
        return new DatabasedMongo(mongo(mongoReadHost, mongoReadPort), mongoReadDbName);
    }

    @Bean
    public DatabasedMongo databasedWriteMongo() {
        return new DatabasedMongo(mongo(mongoWriteHost, mongoWritePort), mongoWriteDbName);
    }

    public Mongo mongo(String mongoHost, Integer mongoPort) {
        Mongo mongo = new MongoClient(
                mongoHosts(mongoHost, mongoPort),
                MongoClientOptions.builder()
                        .connectionsPerHost(1000)
                        .connectTimeout(10000)
                        .build()
        );
        if (processingConfig == null || !processingConfig.toBoolean()) {
            mongo.setReadPreference(ReadPreference.secondaryPreferred());
        }
        return mongo;
    }

    private IdGeneratorBuilder idGeneratorBuilder() {
        return sequenceIdentifier -> new MongoSequentialIdGenerator(
                databasedWriteMongo(),
                sequenceIdentifier
        );
    }

    @Bean
    @Primary
    public ContentIndex contentIndex() {
        return esContentIndexModule().equivContentIndex();
    }

    @Bean
    @Primary
    public EsContentTranslator esContentTranslator() {
        return esContentIndexModule().translator();
    }

    @Bean
    @Primary
    public EsTopicIndex topicIndex() {
        return esContentIndexModule().topicIndex();
    }

    @Bean
    @Primary
    public EsPopularTopicIndex popularTopicIndex() {
        return esContentIndexModule().topicSearcher();
    }

    @Bean
    @Primary
    public EsContentTitleSearcher contentSearcher() {
        return esContentIndexModule().contentTitleSearcher();
    }

    @Bean
    @Primary
    public ServiceChannelStore channelStore() {
        MongoChannelStore rawStore = new MongoChannelStore(
                databasedReadMongo(),
                channelGroupStore(),
                channelGroupStore()
        );
        return new CachingChannelStore(rawStore);
    }

    @Bean
    @Primary
    public ChannelGroupStore channelGroupStore() {
        return new CachingChannelGroupStore(
                new MongoChannelGroupStore(databasedReadMongo())
        );
    }

    private List<ServerAddress> mongoHosts(String mongoHost, final Integer mongoPort) {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(
                splitter.split(mongoHost),
                input -> new ServerAddress(input, mongoPort)
        ), Predicates.notNull()));
    }

    @Bean
    HealthProbe mongoConnectionProbe() {
        return new MongoConnectionPoolProbe();
    }

    @Bean
    public ChannelResolver channelResolver() {
        return new LegacyChannelResolver(channelStore(), new LegacyChannelTransformer());
    }

    @Bean
    public ChannelGroupResolver channelGroupResolver() {
        return new LegacyChannelGroupResolver(
                channelGroupStore(),
                new LegacyChannelGroupTransformer()
        );
    }

    @Bean
    public PlayerResolver playerResolver() {
        return new CachingPlayerResolver(new MongoPlayerStore(databasedReadMongo()));
    }

    @Bean
    public ServiceResolver serviceResolver() {
        return new CachingServiceResolver(new MongoServiceStore(databasedReadMongo()));
    }

    public LegacyContentResolver legacyContentResolver() {
        DatabasedMongo mongoDb = databasedReadMongo();
        KnownTypeContentResolver contentResolver = new MongoContentResolver(
                mongoDb,
                legacyEquivalenceStore()
        );
        return new LegacyContentResolver(
                legacyEquivalenceStore(),
                contentResolver,
                legacyContentTransformer()
        );
    }

    public MongoLookupEntryStore legacyEquivalenceStore() {
        return new MongoLookupEntryStore(
                databasedReadMongo().collection(MONGO_COLLECTION_LOOKUP),
                persistenceAuditLog,
                ReadPreference.primaryPreferred()
        );
    }

    public LegacySegmentMigrator legacySegmentMigrator() {
        return new LegacySegmentMigrator(legacySegmentResolver(), segmentStore());
    }

    private org.atlasapi.media.segment.SegmentResolver legacySegmentResolver() {
        return new MongoSegmentResolver(databasedReadMongo(), new SubstitutionTableNumberCodec());
    }

    public ScheduleWriter v2ScheduleStore() {
        return persistenceModule().v2ScheduleStore();
    }

    @Bean
    @Qualifier("legacy")
    public EventResolver legacyEventResolver() {
        return new LegacyEventResolver(new MongoEventStore(databasedReadMongo()), idSettingOrganisationStore());
    }


    @Bean
    @Qualifier("legacy")
    public ContentListerResourceListerAdapter legacyContentLister() {
        DatabasedMongo mongoDb = databasedReadMongo();
        LegacyContentLister contentLister = new LegacyLookupResolvingContentLister(
                new MongoLookupEntryStore(
                        mongoDb.collection(MONGO_COLLECTION_LOOKUP),
                        persistenceAuditLog,
                        ReadPreference.secondary()
                ),
                new MongoContentResolver(mongoDb, legacyEquivalenceStore())
        );
        return new ContentListerResourceListerAdapter(
                contentLister,
                legacyContentTransformer()
        );
    }

    @Bean
    @Qualifier("legacy")
    public LegacyContentTransformer legacyContentTransformer() {
        return new LegacyContentTransformer(
                channelStore(),
                legacySegmentMigrator(),
                new PaTagMap(
                        legacyTopicStore(),
                        new MongoSequentialIdGenerator(databasedWriteMongo(),
                                MONGO_COLLECTION_TOPICS
                        )));
    }

    @Bean
    @Qualifier("legacy")
    public LegacyTopicLister legacyTopicLister() {
        return new LegacyTopicLister(legacyTopicStore());
    }

    @Bean
    @Qualifier("legacy")
    public MongoTopicStore legacyTopicStore() {
        return new MongoTopicStore(databasedReadMongo(), persistenceAuditLog);
    }

    // disable Bean as this breaks API spring construction
    public LegacyTopicResolver legacyTopicResolver() {
        return new LegacyTopicResolver(legacyTopicStore(), legacyTopicStore());
    }


    @Bean
    @Qualifier("legacy")
    public OrganisationResolver legacyOrganisationResolver() {
        TransitiveLookupWriter lookupWriter = TransitiveLookupWriter.generatedTransitiveLookupWriter(
                legacyEquivalenceStore());
        MongoOrganisationStore store = new MongoOrganisationStore(databasedReadMongo(),
                lookupWriter,
                legacyEquivalenceStore(),
                persistenceAuditLog);
        LegacyOrganisationTransformer transformer = new LegacyOrganisationTransformer();
        return new LegacyOrganisationResolver(store, transformer);
    }


    @Bean
    @Qualifier("legacy")
    public ScheduleResolver legacyScheduleStore() {
        DatabasedMongo db = databasedReadMongo();
        KnownTypeContentResolver contentResolver = new MongoContentResolver(
                db,
                legacyEquivalenceStore()
        );
        LookupResolvingContentResolver resolver = new LookupResolvingContentResolver(
                contentResolver,
                legacyEquivalenceStore()
        );
        EquivalentContentResolver equivalentContentResolver = new DefaultEquivalentContentResolver(
                contentResolver,
                legacyEquivalenceStore()
        );
        MessageSender<ScheduleUpdateMessage> sender = MessageSenders.noopSender();
        return new LegacyScheduleResolver(
                new MongoScheduleStore(
                        db,
                        channelStore(),
                        resolver,
                        equivalentContentResolver,
                        sender
                ),
                legacyContentTransformer()
        );
    }

    @Bean
    public Neo4jModule neo4jModule() {
        return Neo4jModule.create(
            neo4jHost, neo4jPort, neo4jMaxIdleSessions
        );
    }

    @Bean
    public Neo4jContentStore neo4jContentStore() {
        return neo4jModule().neo4jContentStore(metricsModule.metrics());
    }
}
