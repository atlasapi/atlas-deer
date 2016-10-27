package org.atlasapi.util;

import java.io.IOException;
import java.nio.charset.Charset;

import com.metabroadcast.common.persistence.cassandra.DatastaxCassandraService;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.joda.InstantCodec;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.json.JacksonJsonCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraInit {

    private static final Logger log = LoggerFactory.getLogger(CassandraInit.class);

    private static final ImmutableSet<String> SEEDS = ImmutableSet.of("localhost");
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    public static void createTables(Session session, AstyanaxContext<Keyspace> context)
            throws ConnectionException, IOException {
        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS atlas_testing "
                        + "WITH replication = {"
                        + "'class': 'SimpleStrategy', "
                        + "'replication_factor':1"
                        + "};");
        session.execute("USE atlas_testing");

        executeFile(session, "atlas.schema");
        executeFile(session, "content_v2.schema");
        executeFile(session, "atlas_event_v2.schema");
        executeFile(session, "atlas_event_aliases_v2.schema");
        executeFile(session, "atlas_schedule_v2.schema");
        executeFile(session, "atlas_organisation.schema");
        executeFile(session, "atlas_organisation_uri.schema");
    }

    private static void executeFile(Session session, String resource) throws IOException {
        String atlasSchema = Resources.toString(
                Resources.getResource(resource),
                Charset.defaultCharset()
        );

        executeStatementsString(session, atlasSchema);
    }

    private static void executeStatementsString(Session session, String statements) {
        for (String stmt : statements.split(";")) {
            stmt = stmt.trim();

            if (!stmt.isEmpty()) {
                try {
                    session.execute(stmt);
                } catch (Exception e) {
                    log.error("Failed to exec statement {}", stmt, e);
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    public static void nukeIt(Session session) {
        session.execute("DROP KEYSPACE IF EXISTS atlas_testing");
    }

    public static void truncate(Session session, AstyanaxContext<Keyspace> context)
            throws ConnectionException {
        ImmutableList<String> tables = ImmutableList.of(
                "content", "content_aliases", "equivalence_graph_index",
                "equivalence_graph", "segments", "segments_aliases", "schedule_v2", "schedule",
                "equivalent_content_index", "equivalent_content", "equivalent_schedule",
                "organisation", "organisation_uri", "event_v2", "event_aliases_v2", "content_v2"
        );
        for (String table : tables) {
            session.execute(String.format("TRUNCATE %s", table));
        }
    }

    public static DatastaxCassandraService datastaxCassandraService() {
        return DatastaxCassandraService.builder()
                    .withNodes(SEEDS)
                    .withConnectionsPerHostLocal(8)
                    .withConnectionsPerHostRemote(2)
                    .withCodecRegistry(new CodecRegistry()
                            .register(InstantCodec.instance)
                            .register(LocalDateCodec.instance)
                            .register(new JacksonJsonCodec<>(
                                    org.atlasapi.content.v2.model.Clip.Wrapper.class,
                                    MAPPER
                            ))
                            .register(new JacksonJsonCodec<>(
                                    org.atlasapi.content.v2.model.Encoding.Wrapper.class,
                                    MAPPER
                            ))
                    )
                    .build();
    }
}
