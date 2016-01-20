package org.atlasapi.util;

import java.io.IOException;

import org.atlasapi.entity.CassandraHelper;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.commons.io.IOUtils;

public class CassandraInit {
    public static void createTables(Session session, AstyanaxContext<Keyspace> context)
            throws ConnectionException, IOException {
        session.execute("CREATE KEYSPACE atlas_testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
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

        session.execute("CREATE TABLE equivalence_graph_index (resource_id bigint, graph_id bigint, PRIMARY KEY (resource_id));");
        session.execute("CREATE TABLE equivalence_graph (graph_id bigint, graph blob, PRIMARY KEY (graph_id));");
        session.execute("CREATE TABLE equivalent_content_index (key bigint, value bigint, PRIMARY KEY (key));");
        session.execute("CREATE TABLE equivalent_content (set_id bigint, content_id bigint, graph blob static, data blob, PRIMARY KEY (set_id,content_id));");
        session.execute("CREATE TABLE equivalent_schedule (source text, channel bigint, day timestamp, broadcast_id text, broadcast_start timestamp, broadcast blob, graph blob, content_count bigint, content blob, schedule_update timestamp, equiv_update timestamp, PRIMARY KEY ((source, channel, day), broadcast_id)) ");

        /*
        TODO: this. It doesn't work atm because Dstax sessions can't execute files, they need separate statements
        session.execute(IOUtils.toString(CassandraInit.class.getResourceAsStream("/atlas.schema")));
         */

        session.execute(IOUtils.toString(CassandraInit.class.getResourceAsStream("/atlas_event.schema")));
        session.execute(IOUtils.toString(CassandraInit.class.getResourceAsStream("/atlas_schedule_v2.schema")));
        session.execute(IOUtils.toString(CassandraInit.class.getResourceAsStream("/atlas_organisation.schema")));
        session.execute("CREATE INDEX inverse_equivalent_content_index ON equivalent_content_index(value);");

        CassandraHelper.createColumnFamily(context, "schedule", StringSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, "event_aliases", StringSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, "content", LongSerializer.get(), StringSerializer .get());
        CassandraHelper.createColumnFamily(context, "content_aliases", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    }

    public static void nukeIt(Session session) {
        session.execute("DROP KEYSPACE IF EXISTS atlas_testing");
    }

    public static void truncate(Session session, AstyanaxContext<Keyspace> context)
            throws ConnectionException {
        ImmutableList<String> tables = ImmutableList.of(
                "content", "content_aliases", "event_aliases", "equivalence_graph_index",
                "equivalence_graph", "segments", "segments_aliases", "schedule_v2", "schedule",
                "equivalent_content_index", "equivalent_content", "equivalent_schedule", "event",
                "organisation"
        );
        for (String table : tables) {
            session.execute(String.format("TRUNCATE %s", table));
        }
    }
}
