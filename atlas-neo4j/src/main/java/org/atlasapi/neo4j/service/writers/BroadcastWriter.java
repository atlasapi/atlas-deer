package org.atlasapi.neo4j.service.writers;

import org.atlasapi.content.Item;

import com.google.common.collect.ImmutableMap;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static org.atlasapi.neo4j.service.model.Neo4jBroadcast.BROADCAST;
import static org.atlasapi.neo4j.service.model.Neo4jBroadcast.HAS_BROADCAST_RELATIONSHIP;
import static org.atlasapi.neo4j.service.model.Neo4jBroadcast.CHANNEL_ID;
import static org.atlasapi.neo4j.service.model.Neo4jBroadcast.END_DATE_TIME;
import static org.atlasapi.neo4j.service.model.Neo4jBroadcast.START_DATE_TIME;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;

public class BroadcastWriter extends Neo4jWriter {

    private final Statement removeAllBroadcastsStatement;
    private final Statement addBroadcastStatement;

    private BroadcastWriter() {
        removeAllBroadcastsStatement = new Statement(""
                + "MATCH (content { " + CONTENT_ID + ": " + param(CONTENT_ID) + " })"
                + "-[r:" + HAS_BROADCAST_RELATIONSHIP + "]->(broadcast:" + BROADCAST + ") "
                + "DELETE r, broadcast");

        addBroadcastStatement = new Statement(""
                + "MATCH (content { " + CONTENT_ID + ": " + param(CONTENT_ID) + " }) "
                + "OPTIONAL MATCH "
                + "(content)"
                + "-[r:" + HAS_BROADCAST_RELATIONSHIP + "]->"
                + "(existingBroadcast:" + BROADCAST + ") "
                + "DELETE r, existingBroadcast "
                + "CREATE "
                + "(content)"
                + "-[:" + HAS_BROADCAST_RELATIONSHIP + "]->"
                + "(broadcast:" + BROADCAST + " { "
                + CHANNEL_ID + ": " + param(CHANNEL_ID) + ", "
                + START_DATE_TIME + ": " + param(START_DATE_TIME) + ", "
                + END_DATE_TIME + ": " + param(END_DATE_TIME) + " "
                + "})");
    }

    public static BroadcastWriter create() {
        return new BroadcastWriter();
    }

    public void write(Item item, StatementRunner runner) {
        if (item.getBroadcasts().isEmpty()) {
            write(
                    removeAllBroadcastsStatement.withParameters(ImmutableMap.of(
                            CONTENT_ID, item.getId().longValue()
                    )),
                    runner
            );
        } else {
            item.getBroadcasts()
                    .stream()
                    .map(broadcast -> addBroadcastStatement.withParameters(ImmutableMap.of(
                            CONTENT_ID, item.getId().longValue(),
                            CHANNEL_ID, broadcast.getChannelId().longValue(),
                            START_DATE_TIME, broadcast.getTransmissionTime().toString(),
                            END_DATE_TIME, broadcast.getTransmissionEndTime().toString()
                    )))
                    .forEach(statement -> write(statement, runner));
        }
    }
}
