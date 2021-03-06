package org.atlasapi.equivalence;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.CassandraInit;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.DateTimeZones;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.atlasapi.equivalence.CassandraEquivalenceGraphStore.EQUIVALENCE_GRAPH_INDEX_TABLE;
import static org.atlasapi.equivalence.CassandraEquivalenceGraphStore.GRAPH_ID_KEY;
import static org.atlasapi.equivalence.CassandraEquivalenceGraphStore.RESOURCE_ID_KEY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CassandraEquivalenceGraphStoreIT {

    private static final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private static final String keyspace = "atlas_testing";
    private static final MetricRegistry metricRegistry = new MetricRegistry();
    private static final String metricPrefix = "processing.store";

    private static DatastaxCassandraService service
            = new DatastaxCassandraService(ImmutableList.of("localhost"), 8, 2);
    private static CassandraEquivalenceGraphStore store;
    private static Session session;

    private static MessageSender<EquivalenceGraphUpdateMessage> messageSender = new MessageSender<EquivalenceGraphUpdateMessage>() {

        @Override
        public void sendMessage(EquivalenceGraphUpdateMessage message) {
            //no-op;
        }

        @Override
        public void sendMessage(EquivalenceGraphUpdateMessage message,
                byte[] partitionKey)
                throws MessagingException {
            //no-op
        }

        @Override
        public void close() throws Exception {
            //no-op
        }
    };
    private static AstyanaxContext<Keyspace> context;

    @BeforeClass
    public static void setUp() throws ConnectionException, IOException {
        bbcItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        paItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        itvItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        c4Item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        fiveItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));

        service.startAsync().awaitRunning();
        context = new ConfiguredAstyanaxContext("Atlas", keyspace, seeds, 9160, 5, 60).get();
        context.start();
        session = service.getCluster().connect();
        CassandraInit.createTables(session, context);
        CassandraInit.truncate(session, context);
        store = new CassandraEquivalenceGraphStore(
                messageSender,
                session,
                ConsistencyLevel.ONE,
                ConsistencyLevel.ONE,
                metricRegistry,
                metricPrefix
        );
    }

    @After
    public void truncate() throws ConnectionException {
        CassandraInit.truncate(session, context);
    }

    private static final Item bbcItem = new Item(Id.valueOf(1), Publisher.BBC);
    private static final Item paItem = new Item(Id.valueOf(2), Publisher.PA);
    private static final Item itvItem = new Item(Id.valueOf(3), Publisher.ITV);
    private static final Item c4Item = new Item(Id.valueOf(4), Publisher.C4);
    private static final Item fiveItem = new Item(Id.valueOf(5), Publisher.FIVE);

    @Test
    public void testUpdatingEquivalences() throws Exception {

        Set<Publisher> sources = ImmutableSet.of(
                bbcItem.getSource(),
                paItem.getSource(),
                c4Item.getSource()
        );
        store.updateEquivalences(
                bbcItem.toRef(),
                ImmutableSet.<ResourceRef>of(paItem.toRef(), c4Item.toRef()),
                sources
        );

        sources = ImmutableSet.of(itvItem.getSource(), fiveItem.getSource());
        store.updateEquivalences(
                itvItem.toRef(),
                ImmutableSet.<ResourceRef>of(fiveItem.toRef()),
                sources
        );

        sources = ImmutableSet.of(itvItem.getSource(), paItem.getSource());
        store.updateEquivalences(
                itvItem.toRef(),
                ImmutableSet.<ResourceRef>of(paItem.toRef()),
                sources
        );

        sources = ImmutableSet.of(itvItem.getSource(), paItem.getSource(), fiveItem.getSource());
        store.updateEquivalences(itvItem.toRef(), ImmutableSet.<ResourceRef>of(), sources);

        ListenableFuture<OptionalMap<Id, EquivalenceGraph>> resolveIds = store.resolveIds(
                ImmutableList.of(Id.valueOf(3), Id.valueOf(1)));
        OptionalMap<Id, EquivalenceGraph> graphs = Futures.get(resolveIds, ResolveException.class);

        EquivalenceGraph graph = graphs.get(Id.valueOf(3)).get();
        assertThat(Iterables.getOnlyElement(graph.getEquivalenceSet()), is(Id.valueOf(3)));

        graph = graphs.get(Id.valueOf(1)).get();
        assertThat(
                graph.getEquivalenceSet(),
                is(ImmutableSet.of(Id.valueOf(1), Id.valueOf(2), Id.valueOf(4)))
        );

        resolveIds = store.resolveIds(ImmutableList.of(Id.valueOf(1), Id.valueOf(2)));
        graphs = Futures.get(resolveIds, ResolveException.class);

        EquivalenceGraph graph1 = graphs.get(Id.valueOf(1)).get();
        EquivalenceGraph graph2 = graphs.get(Id.valueOf(2)).get();
        assertEquals(graph1, graph2);
        assertTrue(graph1 == graph2);
    }

    @Test
    public void testCreatingEquivalences() throws Exception {

        ResourceRef subject = new BrandRef(Id.valueOf(1), Publisher.BBC);
        BrandRef equiv = new BrandRef(Id.valueOf(2), Publisher.PA);
        Set<ResourceRef> assertedAdjacents = ImmutableSet.<ResourceRef>of(equiv);
        Set<Publisher> sources = ImmutableSet.of(Publisher.BBC, Publisher.PA);
        store.updateEquivalences(subject, assertedAdjacents, sources);

        ListenableFuture<OptionalMap<Id, EquivalenceGraph>> resolveIds = store.resolveIds(
                ImmutableList.of(Id.valueOf(1)));

        OptionalMap<Id, EquivalenceGraph> graphs = Futures.get(resolveIds, ResolveException.class);
        EquivalenceGraph graph = graphs.get(Id.valueOf(1)).get();
        assertTrue(graph.getAdjacents(Id.valueOf(1)).getOutgoingEdges().contains(equiv));
        assertTrue(graph.getAdjacents(Id.valueOf(2)).getIncomingEdges().contains(subject));
    }

    @Test
    public void testDoNotResolveGraphsFromStaleIndexEntries() throws Exception {
        store.updateEquivalences(
                bbcItem.toRef(), ImmutableSet.of(paItem.toRef()), ImmutableSet.of(Publisher.PA)
        );

        // Add a stale entry to the index, i.e. an entry that points to a graph that does not
        // contain the given resource ID
        session.execute(
                QueryBuilder.insertInto(EQUIVALENCE_GRAPH_INDEX_TABLE)
                        .value(RESOURCE_ID_KEY, itvItem.getId().longValue())
                        .value(GRAPH_ID_KEY, bbcItem.getId().longValue())
        );

        OptionalMap<Id, EquivalenceGraph> optionalMap = store.resolveIds(
                ImmutableList.of(itvItem.getId())
        ).get();

        assertThat(optionalMap.isEmpty(), is(true));
    }
}
