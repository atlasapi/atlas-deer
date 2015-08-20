package org.atlasapi.content;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DatastaxProtobufContentMarshallerTest {

    private final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private final String keyspace = "atlas_testing";
    private final ContentMarshaller<Batch, Iterable<Row>> marshaller = new DatastaxProtobufContentMarshaller(new ContentSerializer(new ContentSerializationVisitor(new NoOpContentResolver())));
    private final AstyanaxContext<Keyspace> context
            = new ConfiguredAstyanaxContext("Atlas", keyspace, seeds, 9160, 5, 60).get();
    private final DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds, 8, 2);
    private Session session;


    @Before
    public void setUp() throws ConnectionException {
        cassandraService.startAsync().awaitRunning();
        context.start();
        session = cassandraService.getCluster().connect();
        session.execute("CREATE KEYSPACE atlas_testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
        session = cassandraService.getCluster().connect("atlas_testing");


        CassandraHelper.createColumnFamily(context, "content", LongSerializer.get(), StringSerializer.get());
    }

    @After
    public void tearDown()  {
        session.execute("DROP KEYSPACE " + keyspace);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testMarshallsAndUnmarshallsContent() {


        Content content = new Episode();
        content.setId(Id.valueOf(1234));
        content.setPublisher(Publisher.BBC);
        content.setTitle("title");
        content.setActivelyPublished(false);
        content.setGenericDescription(true);
        content.setFirstSeen(DateTime.now(DateTimeZone.UTC).minusHours(1));
        content.setLastUpdated(DateTime.now(DateTimeZone.UTC));

        Batch batch = batch();
        marshaller.marshallInto(content.getId(), batch, content);

        session.execute(batch);

        ResultSet rows = session.execute(select().all().from("content").where(eq("key", 1234)));
        Content unmarshalled = marshaller.unmarshallCols(rows);

        assertThat(unmarshalled.getId(), is(content.getId()));
        assertThat(unmarshalled.getTitle(), is(content.getTitle()));
        assertThat(unmarshalled.isActivelyPublished(), is(false));
        assertThat(unmarshalled.isGenericDescription(), is(true));
        assertThat(unmarshalled.getFirstSeen(), is(content.getFirstSeen()));
        assertThat(unmarshalled.getLastUpdated(), is(content.getLastUpdated()));



    }
}