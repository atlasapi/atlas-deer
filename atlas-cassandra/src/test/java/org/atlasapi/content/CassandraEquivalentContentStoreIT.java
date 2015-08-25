package org.atlasapi.content;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import junit.framework.TestSuite;

import org.apache.commons.io.IOUtils;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

@RunWith(AllTests.class)
public class CassandraEquivalentContentStoreIT {

    public static junit.framework.Test suite() throws Exception {
        TestSuite suite = new TestSuite("CassandraEquivalentContentStoreIT");
        
        final TestCassandraPersistenceModule module = new TestCassandraPersistenceModule(){
            @Override
            protected void createTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
                 session.execute("CREATE TABLE atlas_testing.equivalence_graph_index (resource_id bigint, graph_id bigint, PRIMARY KEY (resource_id));");
                 session.execute("CREATE TABLE atlas_testing.equivalence_graph (graph_id bigint, graph blob, PRIMARY KEY (graph_id));");
                 session.execute("CREATE TABLE atlas_testing.equivalent_content_index (key bigint, value bigint, PRIMARY KEY (key));");
//                 session.execute("CREATE TABLE atlas_testing.equivalent_content (set_id bigint, content_id bigint, graph blob, data blob, PRIMARY KEY (set_id,content_id));");
//                 session.execute("CREATE TABLE atlas_testing.equivalent_content (set_id bigint, content_id bigint, graph blob, data blob, PRIMARY KEY (set_id,content_id));");
                try {
                    session.execute(IOUtils.toString(CassandraEquivalentContentStoreIT.class.getResourceAsStream("/atlas_equivalent_content_v2.schema")));
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
                CassandraHelper.createColumnFamily(context, "content", LongSerializer.get(), StringSerializer.get());
                 CassandraHelper.createColumnFamily(context, "content_aliases", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
            }
             
            @Override
            protected void clearTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
                ImmutableList<String> tables = ImmutableList.of(
                     "equivalence_graph_index", "equivalence_graph", 
                     "equivalent_content_index", "equivalent_content");
                for (String table : tables) {
                     session.execute(String.format("TRUNCATE %s", table));
                }
                CassandraHelper.clearColumnFamily(context, "content");
                CassandraHelper.clearColumnFamily(context, "content_aliases");
            }
        };
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
