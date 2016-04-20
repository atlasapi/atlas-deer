package org.atlasapi.neo4j.service.writer;

import java.text.MessageFormat;
import java.util.Optional;

import org.atlasapi.content.Content;
import org.atlasapi.content.Location;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocationWriter {

    private static final String CONTENT_ID = "contentId";
    private static final String LOCATION_NODE_ID = "locationNodeId";
    private static final String AVAILABLE = "available";
    private static final String START_DATE_TIME = "startDateTime";
    private static final String END_DATE_TIME = "endDateTime";

    private final Session session;

    private LocationWriter(Session session) {
        this.session = checkNotNull(session);
    }

    public static LocationWriter create(Session session) {
        return new LocationWriter(session);
    }

    public void deleteExistingLocations(Content content) {
        String query = MessageFormat.format(
                "MATCH (content '{' id: {0} '}')-[:HAS_LOCATION]->(location:Location)\n"
                        + "DETACH DELETE location",
                param(CONTENT_ID)
        );

        session.query(query, ImmutableMap.of(
                CONTENT_ID, content.getId().longValue()
        ));
    }

    public void write(Location location, Content content) {
        Optional<Long> optionalNodeId = writeLocation(location);

        if (optionalNodeId.isPresent()) {
            addLocationToEpisode(optionalNodeId.get(), content);
        }
    }

    private Optional<Long> writeLocation(Location location) {
        if (location.getPolicy() == null) {
            return Optional.empty();
        }

        String query = MessageFormat.format(
                "CREATE (location:Location '{' "
                        + "available: {0}, startDateTime: {1}, endDateTime: {2} '}')\n"
                        + "RETURN id(location) AS id",
                param(AVAILABLE),
                param(START_DATE_TIME),
                param(END_DATE_TIME)
        );

        DateTime availabilityStart = location.getPolicy().getAvailabilityStart();
        DateTime availabilityEnd = location.getPolicy().getAvailabilityEnd();

        Result result = session.query(query, ImmutableMap.of(
                AVAILABLE, location.getAvailable(),
                START_DATE_TIME, availabilityStart != null ? availabilityStart.toString() : "",
                END_DATE_TIME, availabilityEnd != null ? availabilityEnd.toString() : ""
        ));

        if (!result.iterator().hasNext()) {
            throw new RuntimeException("Failed to execute query: " + query);
        }

        Long nodeId = Long.valueOf(result.iterator().next().get("id").toString());
        return Optional.of(nodeId);
    }

    private void addLocationToEpisode(Long locationNodeId, Content content) {
        String query = MessageFormat.format(
                "MATCH (content '{' id: {0} '}'), (location:Location)\n"
                        + "WHERE id(location) = {1}\n"
                        + "MERGE (content)-[r:HAS_LOCATION]->(location)\n"
                        + "RETURN id(r)",
                param(CONTENT_ID),
                param(LOCATION_NODE_ID)
        );

        Result result = session.query(query, ImmutableMap.of(
                CONTENT_ID, content.getId().longValue(),
                LOCATION_NODE_ID, locationNodeId
        ));

        if (!result.iterator().hasNext()) {
            throw new RuntimeException("Failed to execute query: " + query);
        }
    }

    private String param(String param) {
        return "{" + param + "}";
    }
}
