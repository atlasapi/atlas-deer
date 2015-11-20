package org.atlasapi.schedule;

import java.util.concurrent.TimeUnit;

import org.atlasapi.content.ContentStore;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import junit.framework.TestSuite;

@RunWith(AllTests.class)
public class CassandraEquivalentScheduleStoreIT {

    public static junit.framework.Test suite() throws Exception {
        TestSuite suite = new TestSuite("CassandraEquivalentScheduleStoreIT");
        
        final TestCassandraPersistenceModule module = new TestCassandraPersistenceModule();
        module.startAsync().awaitRunning(1, TimeUnit.MINUTES);

        suite.addTest(EquivalentScheduleStoreTestSuiteBuilder
            .using(new EquivalentScheduleStoreSubjectGenerator() {

                @Override
                public EquivalenceGraphStore getEquivalenceGraphStore() {
                    return module.contentEquivalenceGraphStore();
                }
                
                @Override
                public ContentStore getContentStore() {
                    return module.contentStore();
                }

                @Override
                public EquivalentScheduleStore getEquivalentScheduleStore() {
                    return module.equivalentScheduleStore();
                }
                
            })
            .withTearDown(new Runnable() {
                @Override
                public void run() {
                    try {
                        module.reset();
                    } catch (ConnectionException e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .named("CassandraEquivalentScheduleStoreIntegrationSuite")
            .createTestSuite());
        return suite;
     }
    
}
