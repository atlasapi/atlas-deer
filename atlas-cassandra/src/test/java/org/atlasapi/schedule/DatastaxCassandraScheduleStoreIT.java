package org.atlasapi.schedule;

import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxCassandraScheduleStoreIT extends CassandraScheduleStoreIT {

    protected static final String SCHEDULE_CF_NAME = "schedule_v2";
    protected static final Integer CASSANDRA_TIMEOUT_SECONDS = 60;

    private final ConsistencyLevel writeConsistency = ConsistencyLevel.ONE;
    private final ConsistencyLevel readConsistency = ConsistencyLevel.ONE;

    MetricRegistry metricRegistry = new MetricRegistry();
    String metricPrefix = "test.DatastaxCassandraScheduleStoreIT.";

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
                new ItemAndBroadcastSerializer(new ContentSerializer(new ContentSerializationVisitor())),
                CASSANDRA_TIMEOUT_SECONDS,
                metricRegistry,
                metricPrefix
        );
    }

    @Override
    protected String provideScheduleCfName() {
        return SCHEDULE_CF_NAME;
    }
}
