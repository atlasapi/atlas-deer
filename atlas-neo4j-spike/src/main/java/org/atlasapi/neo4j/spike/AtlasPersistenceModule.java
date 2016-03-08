package org.atlasapi.neo4j.spike;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.ElasticSearchContentIndexModule;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.CassandraEquivalentContentStore;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.channel.CachingChannelGroupStore;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.system.legacy.LegacyChannelGroupResolver;
import org.atlasapi.system.legacy.LegacyChannelGroupTransformer;
import org.atlasapi.util.CassandraSecondaryIndex;

import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;

public class AtlasPersistenceModule {

    private static final String mongoReadHost = "db1.owl.atlas.mbst.tv";
    private static final Integer mongoReadPort = 27017;
    private static final String mongoReadDbName = "atlas-split";

    private static final String cassandraCluster = "Atlas";
    private static final String cassandraKeyspace = "atlas_deer";
    private static final String cassandraSeeds = "cassandra1.deer.atlas.mbst.tv,"
                    + "cassandra2.deer.atlas.mbst.tv,"
                    + "cassandra3.deer.atlas.mbst.tv";
    private static final String cassandraPort = "9160";
    private static final String cassandraConnectionTimeout = "1000";

    private static final String cassandraClientThreads = "10";
    private static final Integer cassandraConnectionsPerHostLocal = 2;
    private static final Integer cassandraConnectionsPerHostRemote = 2;
    private static final Integer cassandraDatastaxConnectionTimeout = 1000;
    private static final Integer cassandraDatastaxReadTimeout = 5000;

    private static final String esSeeds = "xlnode4.search.prod.deer.atlas.mbst.tv,"
            + "xlnode5.search.prod.deer.atlas.mbst.tv,"
            + "xlnode6.search.prod.deer.atlas.mbst.tv";
    private static final int port = 9300;
    private static final boolean ssl = false;
    private static final String esCluster = "atlas_deer_20151127";
    private static final String esIndex = "content";
    private static final String esRequestTimeout = "5000";

    private final CassandraPersistenceModule persistenceModule;
    private final ElasticSearchContentIndexModule indexModule;

    public AtlasPersistenceModule() {
        this.persistenceModule = persistenceModule();
        this.persistenceModule.startAsync().awaitRunning();

        this.indexModule = esContentIndexModule();
    }

    public ContentIndex contentIndex() {
        return indexModule.equivContentIndex();
    }

    public EquivalenceGraphStore equivalenceGraphStore() {
        return persistenceModule.equivGraphStore();
    }

    public ContentResolver contentResolver() {
        return persistenceModule.contentStore();
    }

    private ElasticSearchContentIndexModule esContentIndexModule() {
        ElasticSearchContentIndexModule module =
                new ElasticSearchContentIndexModule(
                        esSeeds,
                        port,
                        ssl,
                        esCluster,
                        esIndex,
                        Long.parseLong(esRequestTimeout),
                        persistenceModule.contentStore(),
                        new MetricRegistry(),
                        channelGroupResolver(),
                        new CassandraSecondaryIndex(
                                persistenceModule.getSession(),
                                CassandraEquivalentContentStore.EQUIVALENT_CONTENT_INDEX,
                                persistenceModule.getReadConsistencyLevel()
                        )
                );
        module.init();
        return module;
    }

    private DatabasedMongo databasedReadMongo() {
        return new DatabasedMongo(mongo(mongoReadHost, mongoReadPort), mongoReadDbName);
    }

    private Mongo mongo(String mongoHost, Integer mongoPort) {
        return new MongoClient(
                mongoHosts(mongoHost, mongoPort),
                MongoClientOptions.builder()
                        .connectionsPerHost(1000)
                        .connectTimeout(10000)
                        .build()
        );
    }

    private ChannelGroupStore channelGroupStore() {
        return new CachingChannelGroupStore(
                new MongoChannelGroupStore(databasedReadMongo())
        );
    }

    private List<ServerAddress> mongoHosts(String mongoHost, final Integer mongoPort) {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(
                splitter.split(mongoHost),
                input -> {
                    try {
                        return new ServerAddress(input, mongoPort);
                    } catch (UnknownHostException e) {
                        return null;
                    }
                }
        ), Predicates.notNull()));
    }

    private ChannelGroupResolver channelGroupResolver() {
        return new LegacyChannelGroupResolver(
                channelGroupStore(),
                new LegacyChannelGroupTransformer()
        );
    }

    private CassandraPersistenceModule persistenceModule() {
        Iterable<String> seeds = Splitter.on(",").split(cassandraSeeds);
        ConfiguredAstyanaxContext contextSupplier = new ConfiguredAstyanaxContext(cassandraCluster,
                cassandraKeyspace,
                seeds,
                Integer.parseInt(cassandraPort),
                Integer.parseInt(cassandraClientThreads),
                Integer.parseInt(cassandraConnectionTimeout),
                new MetricRegistry()
        );
        AstyanaxContext<Keyspace> context = contextSupplier.get();
        context.start();

        DatastaxCassandraService cassandraService = DatastaxCassandraService.builder()
                .withNodes(seeds)
                .withConnectionsPerHostLocal(cassandraConnectionsPerHostLocal)
                .withConnectionsPerHostRemote(cassandraConnectionsPerHostRemote)
                .withConnectTimeoutMillis(cassandraDatastaxConnectionTimeout)
                .withReadTimeoutMillis(cassandraDatastaxReadTimeout)
                .build();

        cassandraService.startAsync().awaitRunning();
        return new CassandraPersistenceModule(
                context,
                cassandraService,
                cassandraKeyspace,
                content -> UUID.randomUUID().toString()
        );
    }
}
