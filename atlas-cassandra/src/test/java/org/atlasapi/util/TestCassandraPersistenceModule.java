package org.atlasapi.util;

import java.util.UUID;

import com.codahale.metrics.MetricRegistry;
import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.DatastaxCassandraService;
import org.atlasapi.PersistenceModule;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.segment.SegmentStore;
import org.atlasapi.topic.TopicStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.MessagingException;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public abstract class TestCassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private final String keyspace = "atlas_testing";
    private final MessageSenderFactory messageSenderFactory = new MessageSenderFactory() {

        @Override
        public <M extends Message> MessageSender<M> makeMessageSender(
                String destination, MessageSerializer<? super M> serializer) {
            return new MessageSender<M>() {

                @Override
                public void close() throws Exception {
                }

                @Override
                public void sendMessage(M message) throws MessagingException {
                }
            };
        }
        
    };
    private final ContentHasher hasher = new ContentHasher() {
        
        @Override
        public String hash(Content content) {
            return UUID.randomUUID().toString();
        }
    };

    private final AstyanaxContext<Keyspace> context
        = new ConfiguredAstyanaxContext("Atlas", keyspace, seeds, 9160, 5, 60).get();
    private final DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds, 8, 2);
    
    private CassandraPersistenceModule persistenceModule;
    
    public TestCassandraPersistenceModule() {
        System.setProperty("messaging.destination.equivalence.content.graph.changes", "just-bloody-work");
        System.setProperty("messaging.destination.content.changes", "just-bloody-work");
        System.setProperty("messaging.destination.topics.changes", "just-bloody-work");
        System.setProperty("messaging.destination.schedule.changes", "just-bloody-work");
    }
    
    @Override
    protected void startUp() throws ConnectionException {
        persistenceModule = persistenceModule();
    }
    
    @Override
    protected void shutDown() throws Exception {
        tearDown();
    }

    private CassandraPersistenceModule persistenceModule() throws ConnectionException {
        cassandraService.startAsync().awaitRunning();
        context.start();
        tearDown();
        Session session = cassandraService.getCluster().connect();
        session.execute("CREATE KEYSPACE atlas_testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
        createTables(session, context);
        
        CassandraPersistenceModule persistenceModule = new CassandraPersistenceModule(messageSenderFactory, context, cassandraService,
                keyspace, idGeneratorBuilder(), hasher, seeds, new MetricRegistry());
        persistenceModule.startAsync().awaitRunning();
        return persistenceModule;
    }
    
    protected abstract void createTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException;

    public void tearDown() {
        try {
            Session session = cassandraService.getCluster().connect();
            session.execute("DROP KEYSPACE " + keyspace);
        } catch (InvalidQueryException iqe){
            log.warn("failed to drop " + keyspace);
        }
    }
    
    public void reset() throws ConnectionException {
        Session session = cassandraService.getCluster().connect(keyspace);
        clearTables(session, context);
    }

    protected abstract void clearTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException;

    private IdGeneratorBuilder idGeneratorBuilder() {
        return new IdGeneratorBuilder() {

            @Override
            public IdGenerator generator(String sequenceIdentifier) {
                return new SequenceGenerator();
            }
        };
    }
    
    
    @Override
    public ContentStore contentStore() {
        return persistenceModule.contentStore();
    }

    @Override
    public TopicStore topicStore() {
        return persistenceModule.topicStore();
    }

    @Override
    public ScheduleStore scheduleStore() {
        return persistenceModule.scheduleStore();
    }

    @Override
    public SegmentStore segmentStore() {
        return persistenceModule.segmentStore();
    }

    @Override
    public EquivalenceGraphStore contentEquivalenceGraphStore() {
        return persistenceModule.contentEquivalenceGraphStore();
    }

    @Override
    public EquivalentContentStore equivalentContentStore() {
        return persistenceModule.equivalentContentStore();
    }

    @Override
    public EquivalentScheduleStore equivalentScheduleStore() {
        return persistenceModule.equivalentScheduleStore();
    }

    public Session getCassandraSession() {
        return cassandraService.getSession(keyspace);
    }

}
