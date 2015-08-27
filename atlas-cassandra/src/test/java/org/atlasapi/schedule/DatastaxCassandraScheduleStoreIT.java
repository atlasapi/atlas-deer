package org.atlasapi.schedule;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.commons.io.IOUtils;
import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.entity.CassandraHelper;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxCassandraScheduleStoreIT extends CassandraScheduleStoreIT {

    protected static final String SCHEDULE_CF_NAME = "schedule_v2";
    protected static final Integer CASSANDRA_TIMEOUT_SECONDS = 60;

    private static final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private static final String keyspace = "atlas_testing";
    private static DatastaxCassandraService cassandraService;
    private static  Session session;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;


    @BeforeClass
    public static void setup() throws ConnectionException, IOException {


        CassandraScheduleStoreIT.setup();
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context, CONTENT_CF_NAME, LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_ALIASES_CF_NAME, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
        cassandraService = new DatastaxCassandraService(seeds, 8, 2);
        cassandraService.startAsync().awaitRunning();
        session = cassandraService.getCluster().connect(keyspace);
        DatastaxCassandraScheduleStoreIT.class.getResourceAsStream("atlas_schedule_v2.schema");
        session.execute(IOUtils.toString(DatastaxCassandraScheduleStoreIT.class.getResourceAsStream("/atlas_schedule_v2.schema")));
    }
    @Override
    protected ScheduleStore provideScheduleStore() {

        return new DatastaxCassandraScheduleStore(
                SCHEDULE_CF_NAME,
                contentStore,
                scheduleUpdateSender,
                clock,
                readConsistency,
                writeConsistency,
                session,
                new ItemAndBroadcastSerializer(new ContentSerializer(new ContentSerializationVisitor(contentStore))),
                CASSANDRA_TIMEOUT_SECONDS
        );
    }

    @Override
    protected String provideScheduleCfName() {
        return SCHEDULE_CF_NAME;
    }
}
