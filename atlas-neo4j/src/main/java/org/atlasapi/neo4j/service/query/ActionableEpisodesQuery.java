package org.atlasapi.neo4j.service.query;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.stream.Collectors;

import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ActionableEpisodesQuery implements GraphQuery {

    private final EquivalentSetQuery equivalentSetQuery;
    private final long seriesId;

    private final boolean availabilityFilterEnabled;
    private final ZonedDateTime availableDateTime;

    private final boolean broadcastFilterEnabled;
    private final ZonedDateTime broadcastLessThan;
    private final ZonedDateTime broadcastGreaterThan;

    private final ImmutableSet<Publisher> publishers;

    private ActionableEpisodesQuery(
            IndexQueryParams queryParams,
            Iterable<Publisher> publishers, EquivalentSetQuery equivalentSetQuery
    ) {
        checkArgument(queryParams.getSeriesId().isPresent());
        checkArgument(queryParams.getActionableFilterParams().isPresent());

        this.equivalentSetQuery = checkNotNull(equivalentSetQuery);

        this.seriesId = queryParams.getSeriesId().get().longValue();

        Map<String, String> actionableParams = queryParams.getActionableFilterParams().get();

        this.availabilityFilterEnabled = actionableParams.get("location.available") != null;
        this.availableDateTime = ZonedDateTime.now(ZoneOffset.UTC);

        this.broadcastGreaterThan =
                actionableParams.get("broadcast.time.gt") == null
                ? ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
                : ZonedDateTime.parse(actionableParams.get("broadcast.time.gt"))
                        .withZoneSameInstant(ZoneOffset.UTC);

        this.broadcastLessThan =
                actionableParams.get("broadcast.time.lt") == null
                ? ZonedDateTime.of(3000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
                : ZonedDateTime.parse(actionableParams.get("broadcast.time.lt"))
                        .withZoneSameInstant(ZoneOffset.UTC);

        this.broadcastFilterEnabled =
                actionableParams.get("broadcast.time.gt") != null
                        || actionableParams.get("broadcast.time.lt") != null;

        checkArgument(availabilityFilterEnabled || broadcastFilterEnabled);

        this.publishers = ImmutableSet.copyOf(publishers);
    }

    public static ActionableEpisodesQuery create(IndexQueryParams queryParams,
            Iterable<Publisher> publishers) {
        return new ActionableEpisodesQuery(queryParams, publishers, EquivalentSetQuery.create());
    }

    @Override
    public Neo4jQuery getQuery() {
        ImmutableMap.Builder<String, Object> params = ImmutableMap.builder();

        params.put("seriesId", seriesId);
        params.put(
                "source",
                publishers.stream()
                        .map(Publisher::key)
                        .collect(Collectors.toList())
        );

        if (availabilityFilterEnabled) {
            params.put("availableDateTime", availableDateTime.toString());
        }
        if (broadcastFilterEnabled) {
            params.put("broadcastStartDateTime", broadcastLessThan.toString())
                    .put("broadcastEndDateTime", broadcastGreaterThan.toString());
        }

        String query = entryPoint()
                + "\n"
                + equivalentSetQuery.equivalentSet("series")
                + "\n"
                + episodes()
                + "\n"
                + equivalentSetQuery.equivalentSet("episode")
                + "\n"
                + actionable()
                + "\n"
                + equivalentSetQuery.equivalentSetWithPassThrough("episode", "matched_episode")
                + "\n"
                + groupResults();

        return Neo4jQuery.create(query, params.build());
    }

    private String entryPoint() {
        return "MATCH (a:Series { id: {seriesId} })\n"
                + "USING INDEX a:Series(id)\n"
                + "WITH a AS series\n";
    }

    private String episodes() {
        return "MATCH (series)-[:HAS_CHILD]->(episode:Episode)\n";
    }

    private String actionable() {
        String query = "WHERE episode:Episode\n"
                + "AND episode.source IN {source}\n";

        String locationMatch = "MATCH (episode)-[:HAS_LOCATION]->(location)\n"
                + "WHERE location.startDateTime < {availableDateTime}\n"
                + "AND location.endDateTime > {availableDateTime}\n";

        String broadcastMatch = "MATCH (episode)-[:HAS_BROADCAST]->(broadcast)\n"
                + "WHERE broadcast.startDateTime < {broadcastStartDateTime}\n"
                + "AND broadcast.endDateTime > {broadcastEndDateTime}\n";

        if (availabilityFilterEnabled && broadcastFilterEnabled) {
            query += "OPTIONAL "
                    + locationMatch
                    + "\n"
                    + "OPTIONAL "
                    + broadcastMatch
                    + "\n"
                    + "WITH episode, location, broadcast\n"
                    + "WHERE NOT location IS NULL OR NOT broadcast IS NULL\n";
        }
        else if (availabilityFilterEnabled) {
            query += locationMatch;
        }
        else if (broadcastFilterEnabled) {
            query += broadcastMatch;
        }

        query += "WITH DISTINCT episode\n"
                + "WITH episode, episode AS matched_episode\n";

        return query;
    }

    private String groupResults() {
        return "WITH matched_episode,\n"
                + "reduce(\n"
                + "selected = " + Long.MAX_VALUE + ",\n"
                + "candidate in collect(DISTINCT episode)\n"
                + "|\n"
                + "CASE\n"
                + "WHEN candidate.id < selected AND candidate.source IN {source}\n"
                + "THEN candidate.id\n"
                + "ELSE selected\n"
                + "END\n"
                + ") AS sel_id\n"
                + "WITH DISTINCT sel_id AS id\n"
                + "MATCH (episode:Episode { id: id })\n"
                + "WITH episode\n"
                + "ORDER BY episode.episodeNumber\n"
                + "RETURN episode.id AS id";
    }
}
