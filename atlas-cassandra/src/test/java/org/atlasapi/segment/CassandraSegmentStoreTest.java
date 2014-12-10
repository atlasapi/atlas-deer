package org.atlasapi.segment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class CassandraSegmentStoreTest {

    private final TestCassandraPersistenceModule testPersistenceModule = new TestCassandraPersistenceModule() {
        @Override
        protected void createTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
            session.execute("USE atlas_testing");
            session.execute("CREATE TABLE segments_aliases (\n" +
                    "    key text,\n" +
                    "    column1 text,\n" +
                    "    value bigint,\n" +
                    "    PRIMARY KEY (key, column1)\n" +
                    ") WITH COMPACT STORAGE AND\n" +
                    "    bloom_filter_fp_chance=0.010000 AND\n" +
                    "    caching='KEYS_ONLY' AND\n" +
                    "    comment='' AND\n" +
                    "    dclocal_read_repair_chance=0.000000 AND\n" +
                    "    gc_grace_seconds=864000 AND\n" +
                    "    read_repair_chance=0.100000 AND\n" +
                    "    replicate_on_write='true' AND\n" +
                    "    populate_io_cache_on_flush='false' AND\n" +
                    "    compaction={'class': 'SizeTieredCompactionStrategy'} AND\n" +
                    "    compression={'sstable_compression': 'SnappyCompressor'};");

            session.execute("CREATE TABLE segments (\n" +
                    "  key bigint,\n" +
                    "  column1 text,\n" +
                    "  value blob,\n" +
                    "  PRIMARY KEY (key, column1)\n" +
                    ") WITH COMPACT STORAGE AND\n" +
                    "  bloom_filter_fp_chance=0.010000 AND\n" +
                    "  caching='KEYS_ONLY' AND\n" +
                    "  comment='' AND\n" +
                    "  dclocal_read_repair_chance=0.000000 AND\n" +
                    "  gc_grace_seconds=864000 AND\n" +
                    "  read_repair_chance=0.100000 AND\n" +
                    "  replicate_on_write='true' AND\n" +
                    "  populate_io_cache_on_flush='false' AND\n" +
                    "  compaction={'class': 'SizeTieredCompactionStrategy'} AND\n" +
                    "  compression={'sstable_compression': 'SnappyCompressor'};");
        }

        @Override
        protected void clearTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
            session.execute("DROP KEYSPACE atlas_testing");
        }
    };

    private SegmentStore segmentStore;

    @Before
    public void setUp() throws Exception {
        testPersistenceModule.startAsync().awaitRunning();
        segmentStore = testPersistenceModule.segmentStore();
    }

    @Test
    public void testWriteAndRead() throws Exception {
        Segment segment = new Segment();
        Id id = Id.valueOf(10L);
        segment.setId(id);
        segment.setDuration(Duration.standardSeconds(10L));
        segment.setType(SegmentType.VIDEO);
        segment.setPublisher(Publisher.BBC);
        segmentStore.writeSegment(segment);
        Optional<Segment> writtenSegment = Optional.fromNullable(
                Iterables.getOnlyElement(segmentStore.resolveSegments(ImmutableList.of(id)), null)
        );
        assertThat(writtenSegment.isPresent(), is(true));
        assertThat(writtenSegment.get().equals(segment), is(true));
    }
}