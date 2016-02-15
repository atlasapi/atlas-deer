package org.atlasapi.organisation;

import java.util.concurrent.ExecutionException;

import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class OrganisationUriStoreIT {

    private static final String ORGANISATION_TABLE = "organisation";
    private static final String ORGANISATION_URI_TABLE = "organisation_uri";

    private static final long EXPECTED_ID = 10l;
    private final String canonicalUri = "uri";

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    private static final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private static final String keyspace = "atlas_testing";
    private static final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private static final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;
    private static OrganisationUriStore uriStore;
    private static Session session;
    private static DatastaxCassandraOrganisationStore store;

    private Organisation organisation;

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
        session.execute(IOUtils.toString(DatastaxCassandraOrganizationStoreTest.class.getResourceAsStream(
                "/atlas_organisation_uri.schema")));
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
        uriStore = new OrganisationUriStore(session, writeConsistency, readConsistency);
        store = new DatastaxCassandraOrganisationStore(
                session,
                readConsistency,
                writeConsistency,
                uriStore
        );
        organisation = setupOrganisation();

        store.write(organisation);
    }

    @After
    public void tearDown() throws Exception {
        context.getClient().truncateColumnFamily(ORGANISATION_TABLE);
        context.getClient().truncateColumnFamily(ORGANISATION_URI_TABLE);
    }

    @Test
    public void testReturnIdByUri() throws ExecutionException, InterruptedException {
        Optional<Id> idByUri = uriStore.getIdByUri(organisation).get();
        assertThat(idByUri.get(), is(Id.valueOf(organisation.getId().longValue())));
    }

    @Test
    public void testReturnEmptyOptional() throws ExecutionException, InterruptedException {
        Organisation organisation = new Organisation();
        organisation.setId(Id.valueOf(1l));
        organisation.setPublisher(Publisher.BBC);
        organisation.setCanonicalUri("notexpected");
        Optional<Id> idByUri = uriStore.getIdByUri(organisation).get();
        assertThat(idByUri, is(Optional.absent()));
    }

    private Organisation setupOrganisation() {
        Organisation organisation = new Organisation();
        organisation.setId(EXPECTED_ID);
        organisation.setPublisher(Publisher.OPTA);
        organisation.setCanonicalUri(canonicalUri);
        return organisation;
    }


}