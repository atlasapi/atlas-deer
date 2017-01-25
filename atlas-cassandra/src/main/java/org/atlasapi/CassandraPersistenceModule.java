package org.atlasapi;

import java.util.UUID;

import org.atlasapi.content.AstyanaxCassandraContentStore;
import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.v2.CqlContentStore;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.equivalence.CassandraEquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.event.ConcreteEventStore;
import org.atlasapi.event.DatastaxCassandraEventStore;
import org.atlasapi.event.EventHasher;
import org.atlasapi.event.EventPersistenceStore;
import org.atlasapi.event.EventStore;
import org.atlasapi.hashing.content.ContentHasher;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.organisation.DatastaxCassandraOrganisationStore;
import org.atlasapi.organisation.IdSettingOrganisationStore;
import org.atlasapi.organisation.OrganisationStore;
import org.atlasapi.organisation.OrganisationUriStore;
import org.atlasapi.schedule.AstyanaxCassandraScheduleStore;
import org.atlasapi.schedule.CassandraEquivalentScheduleStore;
import org.atlasapi.schedule.DatastaxCassandraScheduleStore;
import org.atlasapi.schedule.EquivalentScheduleStore;
import org.atlasapi.schedule.ItemAndBroadcastSerializer;
import org.atlasapi.schedule.ScheduleStore;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.atlasapi.segment.CassandraSegmentStore;
import org.atlasapi.segment.Segment;
import org.atlasapi.topic.CassandraTopicStore;
import org.atlasapi.topic.Topic;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.SystemClock;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

    private static final String METRIC_PREFIX = "persistence.store.";

    private String contentEquivalenceGraphChanges = Configurer.get(
            "messaging.destination.equivalence.content.graph.changes").get();
    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private Integer cassandraTimeoutSeconds = Configurer.get(
            "cassandra.schedule.timeout.seconds",
            "60"
    ).toInt();

    private Boolean processing = Objects.firstNonNull(
            Configurer.get("processing.config"),
            Parameter.valueOf("false")
    ).toBoolean();

    private final String ORGANISATION = "organisation";

    private final String keyspace;
    private final Session session;

    private final ContentHasher contentHasher;
    private final EventHasher eventHasher;
    private final IdGeneratorBuilder idGeneratorBuilder;

    private final AstyanaxContext<Keyspace> context;
    private final MetricRegistry metrics;
    private final IdGenerator contentIdGenerator;

    private CassandraTopicStore topicStore;
    private AstyanaxCassandraScheduleStore scheduleStore;
    private CassandraSegmentStore segmentStore;
    private DatastaxCassandraService dataStaxService;
    private CassandraEquivalenceGraphStore contentEquivalenceGraphStore;
    private CassandraEquivalenceGraphStore nullMessageSendingEquivalenceGraphStore;

    private CassandraEquivalenceGraphStore nullMessageSendingEquivGraphStore;
    private CassandraEquivalentScheduleStore equivalentScheduleStore;
    private DatastaxCassandraScheduleStore v2ScheduleStore;
    private AstyanaxCassandraContentStore astyanaxContentStore;
    private CqlContentStore cqlContentStore;
    private EventStore eventStore;
    private OrganisationStore organisationStore;
    private OrganisationStore idSettingOrganisationStore;

    private MessageSenderFactory messageSenderFactory;
    private CqlContentStore nullMessageSendingCqlContentStore;

    public CassandraPersistenceModule(
            MessageSenderFactory messageSenderFactory,
            AstyanaxContext<Keyspace> context,
            DatastaxCassandraService datastaxCassandraService,
            String keyspace,
            IdGeneratorBuilder idGeneratorBuilder,
            ContentHasher contentHasher,
            EventHasher eventHasher,
            Iterable<String> cassNodes,
            MetricRegistry metrics) {
        this.contentHasher = contentHasher;
        this.eventHasher = checkNotNull(eventHasher);
        this.idGeneratorBuilder = idGeneratorBuilder;
        this.contentIdGenerator = idGeneratorBuilder.generator("content");
        this.messageSenderFactory = messageSenderFactory;
        this.dataStaxService = datastaxCassandraService;
        this.keyspace = keyspace;
        this.context = context;
        this.metrics = checkNotNull(metrics);
        this.session = dataStaxService.getSession(keyspace);
    }

    private Equivalence<Segment> segmentEquivalence() {
        return new Equivalence<Segment>() {

            @Override
            protected boolean doEquivalent(Segment target, Segment candidate) {
                return target.getId().equals(candidate.getId());
            }

            @Override
            protected int doHash(Segment segment) {
                return 0;
            }
        };
    }

    public <T extends Message> MessageSender<T> nullMessageSender(Class<T> msgType) {
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

    @Override
    protected void startUp() throws Exception {
        com.datastax.driver.core.ConsistencyLevel read = getReadConsistencyLevel();
        com.datastax.driver.core.ConsistencyLevel write = getWriteConsistencyLevel();
        ConsistencyLevel readConsistency = processing
                                           ? ConsistencyLevel.CL_QUORUM
                                           : ConsistencyLevel.CL_ONE;

        this.contentEquivalenceGraphStore = new CassandraEquivalenceGraphStore(
                sender(contentEquivalenceGraphChanges, EquivalenceGraphUpdateMessage.class),
                session,
                read,
                write,
                metrics,
                METRIC_PREFIX + "CassandraEquivalenceGraphStore."
        );

        this.astyanaxContentStore = makeAstyanaxContentStore(readConsistency);

        this.cqlContentStore = makeCqlContentStore(session);

        this.nullMessageSendingCqlContentStore = makeNullMessageCqlContentStore(session);

        this.nullMessageSendingEquivalenceGraphStore = new CassandraEquivalenceGraphStore(
                nullMessageSender(EquivalenceGraphUpdateMessage.class),
                session,
                read,
                write,
                metrics,
                METRIC_PREFIX + "NullMessageSendingCassandraEquivalenceGraphStore."
        );

        this.equivalentScheduleStore = new CassandraEquivalentScheduleStore(
                contentEquivalenceGraphStore,
                cqlContentStore,
                session,
                read,
                write,
                new SystemClock(),
                metrics,
                METRIC_PREFIX + "CassandraEquivalenceScheduleStore."
        );
        this.nullMessageSendingEquivGraphStore = new CassandraEquivalenceGraphStore(
                nullMessageSender(
                        EquivalenceGraphUpdateMessage.class),
                session,
                read,
                write,
                metrics,
                METRIC_PREFIX + "NullMessageSendingCassandraEquivalenceGraphStore."
        );
        this.v2ScheduleStore = new DatastaxCassandraScheduleStore(
                "schedule_v2",
                cqlContentStore,
                sender(scheduleChanges, ScheduleUpdateMessage.class),
                new SystemClock(),
                getReadConsistencyLevel(),
                getWriteConsistencyLevel(),
                session,
                new ItemAndBroadcastSerializer(new ContentSerializer(new ContentSerializationVisitor())),
                cassandraTimeoutSeconds,
                metrics,
                METRIC_PREFIX + "DatastaxCassandraScheduleStore."
        );
        this.topicStore = CassandraTopicStore.builder(context,
                "topics",
                topicEquivalence(),
                sender(topicChanges, ResourceUpdatedMessage.class),
                idGeneratorBuilder.generator(
                        "topic")
        )
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "CassandraTopicStore.")
                .build();
        this.scheduleStore = AstyanaxCassandraScheduleStore.builder(context, "schedule",
                cqlContentStore, sender(scheduleChanges, ScheduleUpdateMessage.class)
        )
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "AstyanaxCassandraScheduleStore.")
                .build();

        this.segmentStore = CassandraSegmentStore.builder()
                .withKeyspace(keyspace)
                .withTableName("segments")
                .withAliasIndex(AliasIndex.create(context.getClient(), "segments_aliases"))
                .withCassandraSession(getSession())
                .withIdGenerator(idGeneratorBuilder.generator("segment"))
                .withMessageSender(nullMessageSender(ResourceUpdatedMessage.class))
                .withEquivalence(segmentEquivalence())
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "CassandraSegmentStore.")
                .build();

        this.eventStore = getEventStore(session);
        this.organisationStore = getOrganisationStore(session);
        this.idSettingOrganisationStore = getIdSettingOrganisationStore(session);
    }

    private AstyanaxCassandraContentStore makeAstyanaxContentStore(
            ConsistencyLevel readConsistency) {
        return AstyanaxCassandraContentStore.builder(
                context,
                "content",
                contentHasher,
                nullMessageSender(ResourceUpdatedMessage.class),
                contentIdGenerator,
                contentEquivalenceGraphStore
        )
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "AstyanaxCassandraContentStore.")
                .build();
    }

    private CqlContentStore makeCqlContentStore(Session session) {
        return CqlContentStore.builder()
                .withSession(session)
                .withIdGenerator(contentIdGenerator)
                .withClock(new SystemClock())
                .withHasher(contentHasher)
                .withGraphStore(contentEquivalenceGraphStore)
                .withSender(sender(contentChanges, ResourceUpdatedMessage.class))
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "CqlContentStore.")
                .build();
    }

    private CqlContentStore makeNullMessageCqlContentStore(Session session) {
        return CqlContentStore.builder()
                .withSession(session)
                .withIdGenerator(contentIdGenerator)
                .withClock(new SystemClock())
                .withHasher(content -> UUID.randomUUID().toString())
                .withGraphStore(contentEquivalenceGraphStore)
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "NullMessageCqlContentStore.")
                .build();
    }

    public EquivalenceGraphStore nullMessageSendingGraphStore() {
        return nullMessageSendingEquivGraphStore;
    }

    public <M extends Message> MessageSender<M> sender(String dest, Class<M> type) {
        return new MessageSender<M>() {

            private final MessageSender<M> delegate =
                    messageSenderFactory.makeMessageSender(
                            dest,
                            JacksonMessageSerializer.forType(type)
                    );
            private final Timer timer = metrics.timer(dest);

            @Override
            public void sendMessage(M message) throws MessagingException {
                Timer.Context time = timer.time();
                delegate.sendMessage(message);
                time.stop();
            }

            @Override
            public void sendMessage(M message, byte[] partitionKey)
                    throws MessagingException {
                Timer.Context time = timer.time();
                delegate.sendMessage(message, partitionKey);
                time.stop();
            }

            @Override
            public void close() throws Exception {
                delegate.close();
            }
        };
    }

    @Override
    protected void shutDown() throws Exception {
        context.shutdown();
    }

    public AstyanaxContext<Keyspace> getContext() {
        return this.context;
    }

    @Override
    public ContentStore contentStore() {
        return cqlContentStore;
    }

    public ContentStore nullMessageSendingContentStore() {
        return nullMessageSendingCqlContentStore;
    }

    public AstyanaxCassandraContentStore astyanaxContentStore() {
        return astyanaxContentStore;
    }

    @Override
    public CassandraTopicStore topicStore() {
        return topicStore;
    }

    @Override
    public AstyanaxCassandraScheduleStore scheduleStore() {
        return this.scheduleStore;
    }

    @Override
    public ScheduleStore v2ScheduleStore() {
        return v2ScheduleStore;
    }

    @Override
    public CassandraSegmentStore segmentStore() {
        return this.segmentStore;
    }

    public EventStore eventStore() {
        return eventStore;
    }

    @Override
    public OrganisationStore organisationStore() {
        return organisationStore;
    }

    public OrganisationStore idSettingOrganisationStore() {
        return idSettingOrganisationStore;
    }

    private Equivalence<? super Topic> topicEquivalence() {
        return new Equivalence<Topic>() {

            @Override
            protected boolean doEquivalent(Topic a, Topic b) {
                return false;
            }

            @Override
            protected int doHash(Topic t) {
                return 0;
            }
        };
    }

    public Session getSession() {
        return session;
    }

    public EquivalenceGraphStore contentEquivalenceGraphStore() {
        return this.contentEquivalenceGraphStore;
    }

    public EquivalenceGraphStore nullMessageSendingEquivalenceGraphStore() {
        return this.nullMessageSendingEquivalenceGraphStore;
    }

    public com.datastax.driver.core.ConsistencyLevel getReadConsistencyLevel() {
        return processing ? com.datastax.driver.core.ConsistencyLevel.QUORUM
                          : com.datastax.driver.core.ConsistencyLevel.ONE;
    }

    public com.datastax.driver.core.ConsistencyLevel getWriteConsistencyLevel() {
        return com.datastax.driver.core.ConsistencyLevel.QUORUM;
    }

    public EquivalentScheduleStore equivalentScheduleStore() {
        return this.equivalentScheduleStore;
    }

    private EventStore getEventStore(Session session) {
        EventPersistenceStore eventV2PersistenceStore = DatastaxCassandraEventStore.builder()
                .withAliasIndex(AliasIndex.create(context.getClient(), "event_aliases_v2"))
                .withSession(session)
                .withWriteConsistency(getWriteConsistencyLevel())
                .withReadConsistency(getReadConsistencyLevel())
                .build();

        return ConcreteEventStore.builder()
                .withClock(new SystemClock())
                .withIdGenerator(idGeneratorBuilder.generator("event"))
                .withEventHasher(eventHasher)
                .withSender(nullMessageSender(ResourceUpdatedMessage.class))
                .withPersistenceStore(eventV2PersistenceStore)
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "ConcreteEventStore.")
                .build();

    }

    private OrganisationStore getOrganisationStore(Session session) {
        OrganisationUriStore organisationUriStore = OrganisationUriStore.builder()
                .withSession(session)
                .withWriteConsistency(getWriteConsistencyLevel())
                .withReadConsistency(getReadConsistencyLevel())
                .build();

        DatastaxCassandraOrganisationStore organisationStore = DatastaxCassandraOrganisationStore.builder()
                .withSession(session)
                .withWriteConsistency(getWriteConsistencyLevel())
                .withReadConsistency(getReadConsistencyLevel())
                .withOrganisationUriStore(organisationUriStore)
                .withMetricRegistry(metrics)
                .withMetricPrefix(METRIC_PREFIX + "DatastaxCassandraOrganisationStore.")
                .build();

        return organisationStore;
    }

    private OrganisationStore getIdSettingOrganisationStore(Session session) {
        return new IdSettingOrganisationStore(getOrganisationStore(session),idGeneratorBuilder.generator(ORGANISATION));
    }
}
