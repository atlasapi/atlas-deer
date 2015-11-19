package org.atlasapi.schedule;

import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.netflix.astyanax.model.ConsistencyLevel;

@RunWith(MockitoJUnitRunner.class)
public class AstyanaxCassandraScheduleStoreIT extends CassandraScheduleStoreIT {

    protected static final String SCHEDULE_CF_NAME = "schedule";

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
