package org.atlasapi.content;

import java.util.concurrent.TimeUnit;

import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import junit.framework.TestSuite;

@RunWith(AllTests.class)
public class CassandraEquivalentContentStoreIT {

    public static junit.framework.Test suite() throws Exception {
        TestSuite suite = new TestSuite("CassandraEquivalentContentStoreIT");
        
        final TestCassandraPersistenceModule module = new TestCassandraPersistenceModule();
        module.startAsync().awaitRunning(1, TimeUnit.MINUTES);

        suite.addTest(EquivalentContentStoreTestSuiteBuilder
            .using(new EquivalentContentStoreSubjectGenerator() {
                
                @Override
                public EquivalentContentStore getEquivalentContentStore() {
                    return module.equivalentContentStore();
                }
                
                @Override
                public EquivalenceGraphStore getEquivalenceGraphStore() {
                    return module.contentEquivalenceGraphStore();
                }
                
                @Override
                public ContentStore getContentStore() {
                    return module.contentStore();
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
            .named("CassandraEquivalentContentStoreIntegrationSuite")
            .createTestSuite());
        return suite;
     }
}
