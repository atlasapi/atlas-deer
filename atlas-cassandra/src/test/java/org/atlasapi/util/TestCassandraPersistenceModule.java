package org.atlasapi.util;

import java.io.IOException;
import java.util.UUID;

import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.PersistenceModule;
import org.atlasapi.content.CassandraEquivalentContentStore;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.event.EventStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.segment.SegmentStore;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.topic.TopicStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.MessagingException;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class TestCassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

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
        try {
            persistenceModule = persistenceModule();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
    
    @Override
    protected void shutDown() throws Exception {
        tearDown();
    }

    private CassandraPersistenceModule persistenceModule() throws ConnectionException, IOException {
        cassandraService.startAsync().awaitRunning();
        context.start();
        tearDown();
        Session session = cassandraService.getCluster().connect();
        CassandraInit.createTables(session, context);

        CassandraPersistenceModule persistenceModule = new CassandraPersistenceModule(
                messageSenderFactory, context, cassandraService, keyspace, idGeneratorBuilder(),
                content -> UUID.randomUUID().toString(), event -> UUID.randomUUID().toString(),
                seeds, new MetricRegistry());
        persistenceModule.startAsync().awaitRunning();
        return persistenceModule;
    }

    public void tearDown() {
        try {
            Session session = cassandraService.getCluster().connect();
            CassandraInit.nukeIt(session);
        } catch (InvalidQueryException iqe){
            log.warn("failed to drop " + keyspace);
        }
    }
    
    public void reset() throws ConnectionException {
        Session session = cassandraService.getCluster().connect(keyspace);
        clearTables(session, context);
    }

    protected void clearTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
        CassandraInit.truncate(session, context);
    }

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

    public EquivalentContentStore equivalentContentStore() {
        return new CassandraEquivalentContentStore(
                contentStore(),
                new NoOpLegacyContentResolver(),
                persistenceModule.contentEquivalenceGraphStore(),
                messageSenderFactory.makeMessageSender("EquivContentUpdates", null),
                cassandraService.getCluster().connect(keyspace),
                ConsistencyLevel.ONE,
                ConsistencyLevel.ONE

        );
    }

    @Override
    public EquivalentScheduleStore equivalentScheduleStore() {
        return persistenceModule.equivalentScheduleStore();
    }

    @Override
    public ScheduleStore v2ScheduleStore() {
        return persistenceModule.v2ScheduleStore();
    }

    @Override
    public EventStore eventStore() {
        return persistenceModule.eventStore();
    }

    public Session getCassandraSession() {
        return cassandraService.getSession(keyspace);
    }

    private static final class NoOpLegacyContentResolver extends LegacyContentResolver {

    }
}

