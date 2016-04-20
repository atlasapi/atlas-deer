package org.atlasapi.neo4j.spike;

import org.atlasapi.content.AstyanaxCassandraContentStore;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.equivalence.CassandraEquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.messaging.ResourceUpdatedMessage;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;

import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

public class Neo4jCassandraPersistenceModule extends AbstractIdleService {

    private final String keyspace;
    private final Session session;

    private final ContentHasher contentHasher;

    private final AstyanaxContext<Keyspace> context;

    private DatastaxCassandraService dataStaxService;

    private CassandraEquivalenceGraphStore equivGraphStore;
    private ContentResolver contentStore;

    public Neo4jCassandraPersistenceModule(
            AstyanaxContext<Keyspace> context,
            DatastaxCassandraService datastaxCassandraService,
            String keyspace,
            ContentHasher contentHasher) {
        this.contentHasher = contentHasher;
        this.dataStaxService = datastaxCassandraService;
        this.keyspace = keyspace;
        this.context = context;
        this.session = dataStaxService.getSession(keyspace);
    }

    @Override
    protected void startUp() throws Exception {
        Session session = dataStaxService.getSession(keyspace);
        com.datastax.driver.core.ConsistencyLevel read = getReadConsistencyLevel();
        com.datastax.driver.core.ConsistencyLevel write = getWriteConsistencyLevel();
        ConsistencyLevel readConsistency = ConsistencyLevel.CL_ONE;

        this.contentStore = AstyanaxCassandraContentStore.builder(
                context,
                "content",
                contentHasher,
                nullMessageSender(ResourceUpdatedMessage.class),
                getIdGenerator()
        )
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.equivGraphStore = new CassandraEquivalenceGraphStore(
                nullMessageSender(
                        EquivalenceGraphUpdateMessage.class),
                session,
                read,
                write
        );
    }

    public ContentResolver contentStore() {
        return contentStore;
    }

    public EquivalenceGraphStore equivGraphStore() {
        return equivGraphStore;
    }

    public Session getSession() {
        return session;
    }

    public com.datastax.driver.core.ConsistencyLevel getReadConsistencyLevel() {
        return com.datastax.driver.core.ConsistencyLevel.ONE;
    }

    public com.datastax.driver.core.ConsistencyLevel getWriteConsistencyLevel() {
        return com.datastax.driver.core.ConsistencyLevel.QUORUM;
    }

    @Override
    protected void shutDown() throws Exception {
        context.shutdown();
    }

    private <T extends Message> MessageSender<T> nullMessageSender(Class<T> msgType) {
        return new MessageSender<T>() {

            @Override
            public void sendMessage(T resourceUpdatedMessage) throws MessagingException {
            }

            @Override
            public void sendMessage(T message, byte[] partitionKey) throws MessagingException {
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    private IdGenerator getIdGenerator() {
        return new IdGenerator() {

            @Override
            public String generate() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long generateRaw() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
