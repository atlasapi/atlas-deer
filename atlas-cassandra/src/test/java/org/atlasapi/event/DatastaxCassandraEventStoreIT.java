package org.atlasapi.event;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.organisation.OrganisationRef;
import org.atlasapi.util.CassandraInit;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxCassandraEventStoreIT {

    private static final String EVENT_TABLE = "event_v2";
    private static final String EVENT_ALIASES_TABLE = "event_aliases_v2";

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    private static final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private static final String keyspace = "atlas_testing";
    private static final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private static final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;

    private static Session session;

    private @Mock EventHasher hasher;
    private @Mock IdGenerator idGenerator;
    private @Mock MessageSender<ResourceUpdatedMessage> sender;

    private @Mock Clock clock;

    private MetricRegistry metricRegistry;

    private String metricPrefix;
    private EventStore store;
    private DateTime now;
    private DateTime secondNow;
    private long firstId;
    private long secondId;

    @BeforeClass
    public static void init() throws Exception {
        // Thrift init
        context.start();

        // CQL init
        DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds, 8, 2);
        cassandraService.startAsync().awaitRunning();
        session = cassandraService.getCluster().connect();
        CassandraInit.createTables(session, context);
        CassandraInit.truncate(session, context);
        session = cassandraService.getCluster().connect(keyspace);
    }

    @Before
    public void setUp() throws Exception {
        DatastaxCassandraEventStore persistenceStore = new DatastaxCassandraEventStore(
                AliasIndex.create(context.getClient(), EVENT_ALIASES_TABLE),
                session,
                readConsistency,
                writeConsistency
        );

        metricRegistry = new MetricRegistry();
        metricPrefix = "ConcreteEventStore";

        store = new ConcreteEventStore(clock, idGenerator, hasher, sender, persistenceStore, metricRegistry, metricPrefix);

        now = new DateTime(DateTimeZones.UTC);
        secondNow = now.plusHours(1);
        firstId = 4444L;
        secondId = 5555L;

        when(clock.now()).thenReturn(now, secondNow);
        when(idGenerator.generateRaw()).thenReturn(firstId, secondId);
    }

    @After
    public void tearDown() throws Exception {
        context.getClient().truncateColumnFamily(EVENT_TABLE);
    }

    @Test
    public void testWriteAndReadEvent() throws Exception {
        List<OrganisationRef> organisationRefs = ImmutableList.of(new OrganisationRef(Id.valueOf(1l), Publisher.BBC));
        Event expected = Event.builder()
                .withTitle("title")
                .withSource(Publisher.BBC)
                .withOrganisations(organisationRefs).build();
        WriteResult<Event, Event> writeResult = store.write(expected);
        assertTrue(writeResult.written());
        assertThat(writeResult.getResource().getId().longValue(), is(firstId));
        assertThat(writeResult.getResource().getOrganisations(), is(organisationRefs));
        assertFalse(writeResult.getPrevious().isPresent());

        Resolved<Event> resolved = store
                .resolveIds(ImmutableList.of(Id.valueOf(expected.getId().longValue())))
                .get(1, TimeUnit.SECONDS);
        Event actual = Iterables.getOnlyElement(resolved.getResources());

        assertThat(actual.getId(), is(writeResult.getResource().getId()));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getLastUpdated(), is(now));
    }

    @Test
    public void testContentNotWrittenWhenHashNotChanged() throws Exception {
        when(hasher.hash(any())).thenReturn("sameHash");

        Event expected = Event.builder().withTitle("title").withSource(Publisher.BBC).build();

        WriteResult<Event, Event> firstWriteResult = store.write(expected);
        assertTrue(firstWriteResult.written());

        WriteResult<Event, Event> secondWriteResult = store.write(expected);
        assertFalse(secondWriteResult.written());

        verify(hasher, times(2)).hash(any());
    }

    @Test
    public void testContentWrittenWhenHashChanged() throws Exception {
        when(hasher.hash(any())).thenReturn("hashA", "hashB");

        Event expected = Event.builder().withTitle("title").withSource(Publisher.BBC).build();

        WriteResult<Event, Event> firstWriteResult = store.write(expected);
        assertTrue(firstWriteResult.written());

        WriteResult<Event, Event> secondWriteResult = store.write(expected);
        assertTrue(secondWriteResult.written());

        verify(hasher, times(2)).hash(any());

        Event actual = secondWriteResult.getResource();
        assertThat(actual.getId().longValue(), is(firstId));
        assertThat(actual.getLastUpdated(), is(secondNow));

    }

    @Test
    public void testResolvesExistingContentByAlias() throws Exception {
        when(hasher.hash(any())).thenReturn("hashA", "hashB");

        Alias alias = new Alias("namespace", "same");
        Event firstEvent = Event.builder()
                .withTitle("titleA")
                .withSource(Publisher.BBC)
                .withAliases(Lists.newArrayList(alias))
                .build();
        Event secondEvent = Event.builder()
                .withTitle("titleB")
                .withSource(Publisher.BBC)
                .withAliases(Lists.newArrayList(alias))
                .build();

        WriteResult<Event, Event> firstWriteResult = store.write(firstEvent);
        assertTrue(firstWriteResult.written());

        WriteResult<Event, Event> secondWriteResult = store.write(secondEvent);
        assertThat(secondWriteResult.written(), is(true));

        Event actualWritten = secondWriteResult.getResource();
        assertThat(actualWritten.getId().longValue(), is(firstId));
        assertThat(actualWritten.getTitle(), is("titleB"));

        Optional<Event> actualPrevious = secondWriteResult.getPrevious();
        assertThat(actualPrevious.isPresent(), is(true));
        assertThat(actualPrevious.get().getId().longValue(), is(firstId));
    }

    @Test
    public void testResolvingMissingEventReturnsEmptyResolved() throws Exception {
        ListenableFuture<Resolved<Event>> resolved =
                store.resolveIds(ImmutableSet.of(Id.valueOf(4321L)));

        assertTrue(resolved.get(1, TimeUnit.SECONDS).getResources().isEmpty());
    }

}