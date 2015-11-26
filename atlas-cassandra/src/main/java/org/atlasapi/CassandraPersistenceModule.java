package org.atlasapi;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.AstyanaxCassandraContentStore;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.equivalence.CassandraEquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.event.ConcreteEventStore;
import org.atlasapi.event.DatastaxCassandraEventStore;
import org.atlasapi.event.EventHasher;
import org.atlasapi.event.EventPersistenceStore;
import org.atlasapi.event.EventStore;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.ResourceUpdatedMessage;
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
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
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ConsistencyLevel;

public class CassandraPersistenceModule extends AbstractIdleService implements PersistenceModule {

    private static final int CQL_PORT = 9042;
    private String contentEquivalenceGraphChanges = Configurer.get("messaging.destination.equivalence.content.graph.changes").get();
    private String contentChanges = Configurer.get("messaging.destination.content.changes").get();
    private String topicChanges = Configurer.get("messaging.destination.topics.changes").get();
    private String scheduleChanges = Configurer.get("messaging.destination.schedule.changes").get();
    private Integer cassandraTimeoutSeconds = Configurer.get("cassandra.schedule.timeout.seconds", "60").toInt();

    private Boolean processing = Objects.firstNonNull(Configurer.get("processing.config"), Parameter.valueOf("false")).toBoolean();

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
    private AstyanaxCassandraContentStore contentStore;
    private AstyanaxCassandraContentStore nullMsgSendingContentStore;
    private EventStore eventStore;

    private MessageSenderFactory messageSenderFactory;


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
        this.metrics = metrics;
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
            public void sendMessage(T resourceUpdatedMessage) throws MessagingException { }

            @Override
            public void close() throws Exception { }
        };
    }

    @Override
    protected void startUp() throws Exception {
        Session session = dataStaxService.getSession(keyspace);
        com.datastax.driver.core.ConsistencyLevel read = getReadConsistencyLevel();
        com.datastax.driver.core.ConsistencyLevel write = getWriteConsistencyLevel();
        ConsistencyLevel readConsistency = processing ? ConsistencyLevel.CL_QUORUM : ConsistencyLevel.CL_ONE;
        this.contentStore = AstyanaxCassandraContentStore.builder(context, "content",
                contentHasher, sender(contentChanges, ResourceUpdatedMessage.class), contentIdGenerator)
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.nullMsgSendingContentStore = AstyanaxCassandraContentStore.builder(context, "content",
                contentHasher, nullMessageSender(ResourceUpdatedMessage.class), contentIdGenerator)
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();

        this.contentEquivalenceGraphStore = new CassandraEquivalenceGraphStore(
                sender(contentEquivalenceGraphChanges, EquivalenceGraphUpdateMessage.class),
                session, read, write
        );
        this.nullMessageSendingEquivalenceGraphStore = new CassandraEquivalenceGraphStore(
                nullMessageSender(EquivalenceGraphUpdateMessage.class),
                session, read, write
        );
        this.equivalentScheduleStore = new CassandraEquivalentScheduleStore(contentEquivalenceGraphStore, contentStore, session, read, write, new SystemClock());
        this.nullMessageSendingEquivGraphStore = new CassandraEquivalenceGraphStore(nullMessageSender(
                EquivalenceGraphUpdateMessage.class), session, read, write);
        this.v2ScheduleStore = new DatastaxCassandraScheduleStore(
                "schedule_v2",
                contentStore,
                sender(scheduleChanges, ScheduleUpdateMessage.class),
                new SystemClock(),
                getReadConsistencyLevel(),
                getWriteConsistencyLevel(),
                session,
                new ItemAndBroadcastSerializer(new ContentSerializer(new ContentSerializationVisitor(contentStore))),
                cassandraTimeoutSeconds
        );
        this.topicStore = CassandraTopicStore.builder(context, "topics",
                topicEquivalence(), sender(topicChanges, ResourceUpdatedMessage.class), idGeneratorBuilder.generator(
                "topic"))
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.scheduleStore = AstyanaxCassandraScheduleStore.builder(context, "schedule", 
                contentStore, sender(scheduleChanges, ScheduleUpdateMessage.class))
                .withReadConsistency(readConsistency)
                .withWriteConsistency(ConsistencyLevel.CL_QUORUM)
                .build();
        this.segmentStore = CassandraSegmentStore.builder()
                .withKeyspace(keyspace)
                .withTableName("segments")
                .withAliasIndex(AliasIndex.<Segment>create(context.getClient(), "segments_aliases"))
                .withCassandraSession(getSession())
                .withIdGenerator(idGeneratorBuilder.generator("segment"))
                .withMessageSender(nullMessageSender(ResourceUpdatedMessage.class))
                .withEquivalence(segmentEquivalence())
                .build();

        this.eventStore = getEventStore(session);
    }

    public EquivalenceGraphStore nullMessageSendingGraphStore() {
        return nullMessageSendingEquivGraphStore;
    }

    public  <M extends Message> MessageSender<M> sender(String dest, Class<M> type) {
        return new MessageSender<M>() {

            private final MessageSender<M> delegate =
                    messageSenderFactory.makeMessageSender(dest, JacksonMessageSerializer.forType(type));
            private final Timer timer = metrics.timer(dest);

            @Override
            public void sendMessage(M message) throws MessagingException {
                Timer.Context time = timer.time();
                delegate.sendMessage(message);
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
    public AstyanaxCassandraContentStore contentStore() {
        return contentStore;
    }

    public AstyanaxCassandraContentStore nullMessageSendingContentStore() {
        return nullMsgSendingContentStore;
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

    @Override
    public EventStore eventStore() {
        return eventStore;
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
       return  processing ? com.datastax.driver.core.ConsistencyLevel.QUORUM
                : com.datastax.driver.core.ConsistencyLevel.ONE;
    }

    public com.datastax.driver.core.ConsistencyLevel getWriteConsistencyLevel() {
        return  com.datastax.driver.core.ConsistencyLevel.QUORUM;
    }

    public EquivalentScheduleStore equivalentScheduleStore() {
        return this.equivalentScheduleStore;
    }
    
    private EventStore getEventStore(Session session) {
        EventPersistenceStore eventPersistenceStore = DatastaxCassandraEventStore.builder()
                .withAliasIndex(AliasIndex.create(context.getClient(), "event_aliases"))
                .withSession(session)
                .withWriteConsistency(getWriteConsistencyLevel())
                .withReadConsistency(getReadConsistencyLevel())
                .build();

        return ConcreteEventStore.builder()
                .withClock(new SystemClock())
                .withIdGenerator(idGeneratorBuilder.generator("event"))
                .withEventHasher(eventHasher)
                .withSender(nullMessageSender(ResourceUpdatedMessage.class))
                .withPersistenceStore(eventPersistenceStore)
                .build();
    }
}
