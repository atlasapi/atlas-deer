package org.atlasapi.schedule;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import org.atlasapi.entity.CassandraHelper;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class AstyanaxCassandraScheduleStoreIT extends CassandraScheduleStoreIT {

    protected static final String SCHEDULE_CF_NAME = "schedule";

    @BeforeClass
    public static void setup() throws ConnectionException, IOException {
        CassandraScheduleStoreIT.setup();
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context,
                SCHEDULE_CF_NAME,
                StringSerializer.get(),
                StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_CF_NAME, LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_ALIASES_CF_NAME, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    }


    @Override
    protected ScheduleStore provideScheduleStore() {
        return AstyanaxCassandraScheduleStore
                .builder(context, SCHEDULE_CF_NAME, contentStore, scheduleUpdateSender)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
    }

    @Override
    protected String provideScheduleCfName() {
        return SCHEDULE_CF_NAME;
    }
}
