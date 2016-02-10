package org.atlasapi.organisation;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxCassandraOrganizationStoreTest {

    private static final String ORGANISATION_TABLE = "organisation";

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    private static final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private static final String keyspace = "atlas_testing";
    private static final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private static final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;

    private static Session session;
    private static DatastaxCassandraOrganisationStore store;

    @BeforeClass
    public static void init() throws Exception {
        // Thrift init
        context.start();
        cleanUp();
        CassandraHelper.createKeyspace(context);
        // CQL init
        DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds, 8, 2);
        cassandraService.startAsync().awaitRunning();
        session = cassandraService.getCluster().connect(keyspace);

        session.execute(IOUtils.toString(DatastaxCassandraOrganizationStoreTest.class
                .getResourceAsStream("/atlas_organisation.schema")));
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        try {
            context.getClient().dropKeyspace();
        } catch (BadRequestException ire) {
            // Nothing to do
        }
    }

    @Before
    public void setUp() throws Exception {
        store = new DatastaxCassandraOrganisationStore(
                session,
                readConsistency,
                writeConsistency
        );
    }

    @After
    public void tearDown() throws Exception {
        context.getClient().truncateColumnFamily(ORGANISATION_TABLE);
    }

    @Test
    public void testWriteAndReadEvent() throws Exception {
        Set<String> titles = ImmutableSet.of("title1", "title2");
        Organisation expected = new Organisation();
        expected.setAlternativeTitles(titles);
        expected.setId(Id.valueOf(1));
        store.write(expected);
        Resolved<Organisation> resolved = store
                .resolveIds(ImmutableList.of(Id.valueOf(expected.getId().longValue())))
                .get(1, TimeUnit.SECONDS);
        Organisation actual = Iterables.getOnlyElement(resolved.getResources());
        assertThat(actual.getAlternativeTitles(), is(titles));
        assertThat(actual.getId(), is(Id.valueOf(1)));
    }
}

