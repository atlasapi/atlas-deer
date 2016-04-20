package org.atlasapi.neo4j.service;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;

import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.ogm.session.Session;

public class ContentGraphServicePerformance {

    @Ignore("Only intended to be run manually")
    @Test
    public void performanceTest() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Session session = Neo4jSessionFactory.createWithHttpDriver(
                Neo4jSessionFactory.NEO4J_HOST,
                Neo4jSessionFactory.NEO4J_PORT
        ).getNeo4jSession();

        ContentGraphService contentGraphService = ContentGraphService.create(session);

        Series series = new Series(Id.valueOf(20414624), Publisher.METABROADCAST);

        int durationVariableTimes = 0;
        for (int i = 0; i < 500; i++) {
            ZonedDateTime start = ZonedDateTime.now();
            contentGraphService.query(
                    getActionableEpisodeQuery(series, ImmutableMap.of(
                            "location.available", "true",
                            "broadcast.time.gt", now.plusHours(i).toString(),
                            "broadcast.time.lt", now.plusHours(i + 2).toString())),
                    Publisher.all(),
                    ImmutableMap.of(
                            "actionableFilterParameters", "location.available:true",
                            "type", "episode",
                            "series.id", "0L"
                    )
            ).get().get();
            ZonedDateTime end = ZonedDateTime.now();

            durationVariableTimes += Duration.between(start, end).getNano() / 1_000;
        }

        System.out.println("Variable times took: " + durationVariableTimes / 500);
    }

    private IndexQueryParams getActionableEpisodeQuery(Series series,
            ImmutableMap<String, String> actionableParameters) {
        return new IndexQueryParams(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Boolean.FALSE,
                Optional.empty(),
                Optional.of(actionableParameters),
                Optional.of(series.getId())
        );
    }
}
