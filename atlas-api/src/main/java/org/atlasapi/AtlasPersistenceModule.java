package org.atlasapi;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.content.EsContentIndex;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.channel.CachingChannelStore;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.messaging.KafkaMessagingModule;
import org.atlasapi.messaging.MessagingModule;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.topic.EsPopularTopicIndex;
import org.atlasapi.topic.EsTopicIndex;
import org.atlasapi.topic.TopicStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoConnectionPoolProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;

@Configuration
@Import({KafkaMessagingModule.class})
public class AtlasPersistenceModule {

    private final String mongoHost = Configurer.get("mongo.host").get();
    private final Integer mongoPort = Configurer.get("mongo.port").toInt();
    private final String mongoDbName = Configurer.get("mongo.name").get();
    
    private final String cassandraCluster = Configurer.get("cassandra.cluster").get();
    private final String cassandraKeyspace = Configurer.get("cassandra.keyspace").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final String cassandraPort = Configurer.get("cassandra.port").get();
    private final String cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout").get();
    private final String cassandraClientThreads = Configurer.get("cassandra.clientThreads").get();
 
    private final String esSeeds = Configurer.get("elasticsearch.seeds").get();
    private final String esCluster = Configurer.get("elasticsearch.cluster").get();
    private final String esRequestTimeout = Configurer.get("elasticsearch.requestTimeout").get();
    private final Parameter processingConfig = Configurer.get("processing.config");

    @Autowired MessagingModule messaging;

    @PostConstruct
    public void init() {
        persistenceModule().startAsync().awaitRunning();
    }

    @Bean
    public CassandraPersistenceModule persistenceModule() {
        Iterable<String> seeds = Splitter.on(",").split(cassandraSeeds);
        ConfiguredAstyanaxContext contextSupplier = new ConfiguredAstyanaxContext(cassandraCluster, cassandraKeyspace, 
                seeds, Integer.parseInt(cassandraPort), 
                Integer.parseInt(cassandraClientThreads), Integer.parseInt(cassandraConnectionTimeout));
        AstyanaxContext<Keyspace> context = contextSupplier.get();
        context.start();
        DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds);
        cassandraService.startAsync().awaitRunning();
        
        return new CassandraPersistenceModule(messaging.messageSenderFactory(),
                context,
                cassandraService,
                cassandraKeyspace,
                idGeneratorBuilder(), new ContentHasher() {
                    @Override
                    public String hash(Content content) {
                        return UUID.randomUUID().toString();
                    }
                });
    }
    
    @Bean
    public ContentStore contentStore() {
        return persistenceModule().contentStore();
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
    public EquivalenceGraphStore getContentEquivalenceGraphStore() {
        return persistenceModule().contentEquivalenceGraphStore();
    }
    
    @Bean
    public EquivalentContentStore getEquivalentContentStore() {
        return persistenceModule().equivalentContentStore();
    }

    @Bean
    public EquivalentScheduleStore getEquivalentScheduleStore() {
        return persistenceModule().equivalentScheduleStore();
    }

    @Bean
    public ElasticSearchContentIndexModule esContentIndexModule() {
        ElasticSearchContentIndexModule module = 
                new ElasticSearchContentIndexModule(esSeeds, esCluster, Long.parseLong(esRequestTimeout));
        module.init();
        return module;
    }

    @Bean @Primary
    public DatabasedMongo databasedMongo() {
        return new DatabasedMongo(mongo(), mongoDbName);
    }

    @Bean @Primary
    public Mongo mongo() {
        Mongo mongo = new MongoClient(mongoHosts());
        if (processingConfig == null || !processingConfig.toBoolean()) {
            mongo.setReadPreference(ReadPreference.secondaryPreferred());
        }
        return mongo;
    }

    @Bean
    public IdGeneratorBuilder idGeneratorBuilder() {
        return new IdGeneratorBuilder() {

            @Override
            public IdGenerator generator(String sequenceIdentifier) {
                return new MongoSequentialIdGenerator(databasedMongo(), sequenceIdentifier);
            }
        };
    }

    @Bean
    @Primary
    public EsContentIndex contentIndex() {
        return esContentIndexModule().contentIndex();
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
    public ChannelStore channelStore() {
        MongoChannelStore rawStore = new MongoChannelStore(databasedMongo(), channelGroupStore(), channelGroupStore());
        return new CachingChannelStore(rawStore);
    }
    
    @Bean
    @Primary
    public ChannelGroupStore channelGroupStore() {
        return new MongoChannelGroupStore(databasedMongo());
    }

    private List<ServerAddress> mongoHosts() {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(splitter.split(mongoHost), 
            new Function<String, ServerAddress>() {
                @Override
                public ServerAddress apply(String input) {
                    try {
                        return new ServerAddress(input, mongoPort);
                    } catch (UnknownHostException e) {
                        return null;
                    }
                }
            }
        ), Predicates.notNull()));
    }
    
    @Bean
    HealthProbe mongoConnectionProbe() {
        return new MongoConnectionPoolProbe();
    }

}
