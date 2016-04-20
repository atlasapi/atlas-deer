package org.atlasapi.neo4j.service.writer;

import java.text.MessageFormat;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class BroadcastWriter {

    private static final String CONTENT_ID = "contentId";
    private static final String BROADCAST_NODE_ID = "broadcastNodeId";
    private static final String CHANNEL_ID = "channelId";
    private static final String START_DATE_TIME = "startDateTime";
    private static final String END_DATE_TIME = "endDateTime";

    private final Session session;

    private BroadcastWriter(Session session) {
        this.session = checkNotNull(session);
    }

    public static BroadcastWriter create(Session session) {
        return new BroadcastWriter(session);
    }

    public void deleteExistingBroadcasts(Item item) {
        String query = MessageFormat.format(
                "MATCH (content '{' id: {0} '}')-[:HAS_BROADCAST]->(broadcast:Broadcast)\n"
                        + "DETACH DELETE broadcast",
                param(CONTENT_ID)
        );

        session.query(query, ImmutableMap.of(
                CONTENT_ID, item.getId().longValue()
        ));
    }

    public Long write(Broadcast broadcast, Item item) {
        Long broadcastNodeId = writeBroadcast(broadcast);
        addBroadcastToContent(broadcastNodeId, item);
        return broadcastNodeId;
    }

    private Long writeBroadcast(Broadcast broadcast) {
        String query = MessageFormat.format(
                "CREATE (broadcast:Broadcast '{' "
                        + "channelId: {0}, startDateTime: {1}, endDateTime: {2} '}')\n"
                        + "RETURN id(broadcast) AS id",
                param(CHANNEL_ID),
                param(START_DATE_TIME),
                param(END_DATE_TIME)
        );

        DateTime broadcastStart = broadcast.getTransmissionTime();
        DateTime broadcastEnd = broadcast.getTransmissionEndTime();

        Result result = session.query(query, ImmutableMap.of(
                CHANNEL_ID, broadcast.getChannelId().longValue(),
                START_DATE_TIME, broadcastStart != null ? broadcastStart.toString() : "",
                END_DATE_TIME, broadcastEnd != null ? broadcastEnd.toString() : ""
        ));

        if (!result.iterator().hasNext()) {
            throw new RuntimeException("Failed to execute query: " + query);
        }

        return Long.valueOf(result.iterator().next().get("id").toString());
    }

    private void addBroadcastToContent(Long broadcastNodeId, Item item) {
        String query = MessageFormat.format(
                "MATCH (content '{' id: {0} '}'), (broadcast:Broadcast)\n"
                    + "WHERE id(broadcast) = {1}\n"
                    + "MERGE (content)-[r:HAS_BROADCAST]->(broadcast)\n"
                    + "RETURN id(r)",
                param(CONTENT_ID),
                param(BROADCAST_NODE_ID)
        );

        Result result = session.query(query, ImmutableMap.of(
                CONTENT_ID, item.getId().longValue(),
                BROADCAST_NODE_ID, broadcastNodeId
        ));

        if (!result.iterator().hasNext()) {
            throw new RuntimeException("Failed to execute query: " + query);
        }
    }

    private String param(String param) {
        return "{" + param + "}";
    }
}
