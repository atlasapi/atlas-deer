package org.atlasapi.util;

import java.io.IOException;
import java.nio.charset.Charset;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class CassandraInit {

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
                session.execute(stmt);
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
}
