package org.atlasapi.organisation;

import java.util.concurrent.ExecutionException;

import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IdSettingOrganisationStoreIT {

    private static final String ORGANISATION_TABLE = "organisation";
    private static final String ORGANISATION_URI = "organisation_uri";

    private static final long EXPECTED_ID = 10l;

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    private static final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private static final String keyspace = "atlas_testing";
    private static final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private static final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;
    private static OrganisationUriStore uriStore;

    @Mock
    private IdGenerator idGenerator;
    @Captor
    private ArgumentCaptor<Organisation> organisationArgumentCaptor;

    private static Session session;
    private static DatastaxCassandraOrganisationStore store;
    private static IdSettingOrganisationStore idSettingOrganisationStore;

    @BeforeClass
    public static void init() throws Exception {
        // Thrift init
        context.start();
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

    @Before
    public void setUp() throws Exception {
        uriStore = new OrganisationUriStore(session, writeConsistency, readConsistency);
        store = new DatastaxCassandraOrganisationStore(
                session,
                readConsistency,
                writeConsistency,
                uriStore
        );
        store.write(getOrganisation(1L, "uri"));
        when(idGenerator.generateRaw()).thenReturn(EXPECTED_ID);
        idSettingOrganisationStore = new IdSettingOrganisationStore(store, idGenerator);
    }

    @After
    public void tearDown() throws Exception {
        context.getClient().truncateColumnFamily(ORGANISATION_TABLE);
        context.getClient().truncateColumnFamily(ORGANISATION_URI);
    }

    @Test
    public void testSetIdForNewOrganisation()
            throws InterruptedException, ExecutionException {
        Organisation organisation = getOrganisation(1l, "newuri");
        idSettingOrganisationStore.write(organisation);
        Resolved<Organisation> resolved = store.resolveIds(ImmutableSet.of(Id.valueOf(EXPECTED_ID))).get();
        assertThat(resolved.getResources().size(), is(1));
        Organisation resolvedOrganisation = resolved.getResources().first().get();
        assertThat(resolvedOrganisation.getId(), is(Id.valueOf(EXPECTED_ID)));
        assertThat(resolvedOrganisation.getCanonicalUri(), is("newuri"));
    }

    @Test
    public void testNoSetIdForExistingOrganisation()
            throws InterruptedException, ExecutionException {
        long expectedId = 1l;
        Organisation organisation = getOrganisation(expectedId, "uri");
        idSettingOrganisationStore.write(organisation);
        Resolved<Organisation> resolved = store.resolveIds(ImmutableSet.of(Id.valueOf(expectedId))).get();
        assertThat(resolved.getResources().size(), is(1));
        Organisation resolvedOrganisation = resolved.getResources().first().get();
        assertThat(resolvedOrganisation.getId(), is(Id.valueOf(expectedId)));
        assertThat(resolvedOrganisation.getCanonicalUri(), is("uri"));
    }

    private Organisation getOrganisation(long id, String uri) {
        Organisation organisation = new Organisation();
        organisation.setId(Id.valueOf(id));
        organisation.setCanonicalUri(uri);
        organisation.setPublisher(Publisher.BBC);
        return organisation;
    }
}