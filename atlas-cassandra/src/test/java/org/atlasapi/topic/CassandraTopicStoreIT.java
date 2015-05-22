package org.atlasapi.topic;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

@RunWith(MockitoJUnitRunner.class)
public class CassandraTopicStoreIT {
    
    public class StubbableEquivalence<T> extends Equivalence<T> {

        @Override
        public boolean doEquivalent(T a, T b) {
            return false;
        }

        @Override
        protected int doHash(T t) {
            return 0;
        }

    }

    private static final AstyanaxContext<Keyspace> context =
        CassandraHelper.testCassandraContext();

    @Mock private StubbableEquivalence<Topic> equiv;
    @Mock private IdGenerator idGenerator;
    @Mock private MessageSender<ResourceUpdatedMessage> sender;
    @Mock private Clock clock;

    private CassandraTopicStore topicStore;

    @Before
    public void before() {
        topicStore = CassandraTopicStore
            .builder(context, "topics", equiv, sender, idGenerator)
            .withReadConsistency(ConsistencyLevel.CL_ONE)
            .withWriteConsistency(ConsistencyLevel.CL_ONE)
            .withClock(clock)
        .build();
    }
    
    @BeforeClass
    public static void setup() throws ConnectionException {
        context.start();
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context,
            "topics",
            LongSerializer.get(),
            StringSerializer.get());
        CassandraHelper.createColumnFamily(context,
            "topics_aliases",
            StringSerializer.get(),
            StringSerializer.get());
    }

    @AfterClass
    public static void tearDown() throws ConnectionException {
        context.getClient().dropKeyspace();
    }

    @After
    public void clearCf() throws ConnectionException {
        CassandraHelper.clearColumnFamily(context, "topics");
        CassandraHelper.clearColumnFamily(context, "topics_aliases");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResolveIds() throws Exception {
        Topic topic1 = new Topic();
        topic1.setPublisher(Publisher.DBPEDIA);
        topic1.addAlias(new Alias("dbpedia", "alias"));
        topic1.setType(Topic.Type.UNKNOWN);

        Topic topic2 = new Topic();
        topic2.setPublisher(Publisher.METABROADCAST);
        topic2.addAlias(new Alias("mbst", "alias"));
        topic2.setType(Topic.Type.UNKNOWN);

        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L, 1235L);

        WriteResult<Topic, Topic> topic1result = topicStore.writeTopic(topic1);
        WriteResult<Topic, Topic> topic2result = topicStore.writeTopic(topic2);

        assertThat(topic1result.written(), is(true));
        assertThat(topic2result.written(), is(true));

        verify(equiv, never()).doEquivalent(argThat(isA(Topic.class)), argThat(isA(Topic.class)));
        reset(equiv);

        Id topic1id = topic1result.getResource().getId();
        Id topic2id = topic2result.getResource().getId();

        Resolved<Topic> resolved = topicStore.resolveIds(ImmutableList.of(
            topic1id, topic2id
        )).get(1, TimeUnit.MINUTES);

        OptionalMap<Id, Topic> resolvedMap = resolved.toMap();
        assertThat(resolvedMap.get(topic1id).get().getAliases(), hasItem(new Alias("dbpedia", "alias")));
        assertThat(resolvedMap.get(topic2id).get().getAliases(), hasItem(new Alias("mbst", "alias")));
    }

    @Test
    public void testResolveAliases() {
        Alias sharedAlias = new Alias("shared", "alias");

        Topic topic1 = new Topic();
        topic1.setPublisher(Publisher.DBPEDIA);
        topic1.addAlias(sharedAlias);
        topic1.setType(Topic.Type.UNKNOWN);

        Topic topic2 = new Topic();
        topic2.setPublisher(Publisher.METABROADCAST);
        topic2.addAlias(sharedAlias);
        topic2.setType(Topic.Type.UNKNOWN);

        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L, 1235L);

        topicStore.writeTopic(topic1);
        topicStore.writeTopic(topic2);

        OptionalMap<Alias, Topic> resolved = topicStore.resolveAliases(
            ImmutableList.of(sharedAlias), Publisher.METABROADCAST);
        assertThat(resolved.size(), is(1));
        Topic topic = resolved.get(sharedAlias).get();
        assertThat(topic.getSource(), is(Publisher.METABROADCAST));
        assertThat(topic.getId(), is(Id.valueOf(1235)));

        resolved = topicStore.resolveAliases(
            ImmutableList.of(sharedAlias), Publisher.DBPEDIA);
        assertThat(resolved.size(), is(1));
        topic = resolved.get(sharedAlias).get();
        assertThat(topic.getSource(), is(Publisher.DBPEDIA));
        assertThat(topic.getId(), is(Id.valueOf(1234)));

    }

    @Test
    public void testDoesntRewriteTopicWhenEquivalentToPrevious() {
        Topic topic1 = new Topic();
        topic1.setPublisher(Publisher.DBPEDIA);
        topic1.addAlias(new Alias("shared", "alias"));
        topic1.setType(Topic.Type.UNKNOWN);

        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L, 1235L);

        WriteResult<Topic, Topic> writeResult = topicStore.writeTopic(topic1);

        reset(clock, idGenerator, equiv);
        when(equiv.doEquivalent(any(Topic.class), any(Topic.class)))
            .thenReturn(true);

        WriteResult<Topic, Topic> writeResult2 = topicStore.writeTopic(writeResult.getResource());

        assertThat(writeResult2.written(), is(false));
        
        verify(idGenerator, never()).generateRaw();
        verify(clock, never()).now();
        verify(equiv).doEquivalent(any(Topic.class), any(Topic.class));
    }
    
    @Test
    public void testUpdatesAliasIndexWhenAliasesChange() {
        Topic topic = new Topic();
        topic.setPublisher(Publisher.DBPEDIA);
        Alias alias1 = new Alias("namespace1", "value1");
        topic.addAlias(alias1);
        topic.setType(Topic.Type.UNKNOWN);
        
        DateTime now = new DateTime(DateTimeZones.UTC);
        when(clock.now()).thenReturn(now);
        when(idGenerator.generateRaw()).thenReturn(1234L, 1235L);
        
        WriteResult<Topic, Topic> writeResult = topicStore.writeTopic(topic);

        Topic written = writeResult.getResource();
        
        assertThat(written.getId().longValue(), is(1234L));
        
        Alias alias2 = new Alias("namespace2", "value2");
        written.setAliases(ImmutableList.of(alias2));
        
        writeResult = topicStore.writeTopic(written);
        
        OptionalMap<Alias, Topic> resolved = topicStore.resolveAliases(
                ImmutableList.of(alias1), Publisher.DBPEDIA);
        assertTrue(resolved.isEmpty());

        resolved = topicStore.resolveAliases(
                ImmutableList.of(alias2), Publisher.DBPEDIA);
        assertFalse(resolved.isEmpty());
        
        
    }

}
