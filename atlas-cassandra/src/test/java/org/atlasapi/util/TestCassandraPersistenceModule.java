package org.atlasapi.util;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.extras.codecs.joda.InstantCodec;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.json.JacksonJsonCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
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
import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.PersistenceModule;
import org.atlasapi.comparison.AlwaysFalseComparer;
import org.atlasapi.content.CassandraEquivalentContentStore;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.event.EventStore;
import org.atlasapi.organisation.OrganisationStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.segment.SegmentStore;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.topic.TopicStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class TestCassandraPersistenceModule extends AbstractIdleService
        implements PersistenceModule {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private final String keyspace = "atlas_testing";
    private final String metricPrefix = "processing.store";
    private final MetricRegistry metricRegistry = new MetricRegistry();
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

                @Override
                public void sendMessage(M message, byte[] partitionKey)
                        throws MessagingException {
                }
            };
        }

    };

    private final AstyanaxContext<Keyspace> context
            = new ConfiguredAstyanaxContext("Atlas", keyspace, seeds, 9160, 5, 60).get();
    private final DatastaxCassandraService cassandraService = DatastaxCassandraService.builder()
            .withNodes(seeds)
            .withConnectionsPerHostLocal(8)
            .withConnectionsPerHostRemote(2)
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
                    ))
            ).build();

    private CassandraPersistenceModule persistenceModule;

    public TestCassandraPersistenceModule() {
        System.setProperty(
                "messaging.destination.equivalence.content.graph.changes",
                "just-bloody-work"
        );
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

        CassandraPersistenceModule persistenceModule = CassandraPersistenceModule.builder()
                .withMessageSenderFactory(messageSenderFactory)
                .withAstyanaxContext(context)
                .withDatastaxCassandraService(cassandraService)
                .withKeyspace(keyspace)
                .withIdGeneratorBuilder(idGeneratorBuilder())
                .withContentHasher(content -> UUID.randomUUID().toString())
                .withComparer(new AlwaysFalseComparer())
                .withEventHasher(eventv2 -> UUID.randomUUID().toString())
                .withMetrics(new MetricRegistry())
                .build();
        persistenceModule.startAsync().awaitRunning();
        return persistenceModule;
    }

    public void tearDown() {
        try {
            Session session = cassandraService.getCluster().connect();
            CassandraInit.truncate(session, context);
        } catch (InvalidQueryException | ConnectionException e) {
            log.warn("failed to truncate {}", keyspace);
        }
    }

    public void reset() throws ConnectionException {
        Session session = cassandraService.getCluster().connect(keyspace);
        clearTables(session, context);
    }

    public Session getSession() {
        return cassandraService.getCluster().connect(keyspace);
    }

    private void clearTables(Session session, AstyanaxContext<Keyspace> context)
            throws ConnectionException {
        CassandraInit.truncate(session, context);
    }

    private IdGeneratorBuilder idGeneratorBuilder() {
        return sequenceIdentifier -> new SequenceGenerator();
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
                messageSenderFactory.makeMessageSender("EquivContentGraphUpdates", null),
                cassandraService.getCluster().connect(keyspace),
                ConsistencyLevel.ONE,
                ConsistencyLevel.ONE,
                metricRegistry,
                metricPrefix

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

    @Override
    public OrganisationStore organisationStore() {
        return persistenceModule.organisationStore();
    }

    public Session getCassandraSession() {
        return cassandraService.getSession(keyspace);
    }

    private static final class NoOpLegacyContentResolver extends LegacyContentResolver {

    }
}

