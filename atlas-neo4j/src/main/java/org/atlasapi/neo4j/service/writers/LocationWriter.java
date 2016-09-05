package org.atlasapi.neo4j.service.writers;

import java.util.Objects;

import org.atlasapi.content.Content;
import org.atlasapi.content.Location;

import com.metabroadcast.common.stream.MoreCollectors;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;
import static org.atlasapi.neo4j.service.model.Neo4jLocation.END_DATE_TIME;
import static org.atlasapi.neo4j.service.model.Neo4jLocation.HAS_LOCATION_RELATIONSHIP;
import static org.atlasapi.neo4j.service.model.Neo4jLocation.LOCATION;
import static org.atlasapi.neo4j.service.model.Neo4jLocation.START_DATE_TIME;

public class LocationWriter extends Neo4jWriter {

    public static final DateTime AVAILABLE_FROM_FOREVER =
            new DateTime(1900, 1, 1, 0, 0, DateTimeZone.UTC);
    public static final DateTime AVAILABLE_UNTIL_FOREVER =
            new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC);

    private final Timer timer;
    private final Statement removeAllLocationsStatement;
    private final Statement addLocationStatement;

    private LocationWriter(Timer timer) {
        this.timer = checkNotNull(timer);

        this.removeAllLocationsStatement = new Statement(""
                + "MATCH (content { " + CONTENT_ID + ": " + param(CONTENT_ID) + " })"
                + "-[r:" + HAS_LOCATION_RELATIONSHIP + "]->(location:" + LOCATION + ") "
                + "DELETE r, location");

        this.addLocationStatement = new Statement(""
                + "MATCH (content { " + CONTENT_ID + ": " + param(CONTENT_ID) + " }) "
                + "OPTIONAL MATCH (content)-[r:" + HAS_LOCATION_RELATIONSHIP + "]->"
                + "(existing:" + LOCATION + ") "
                + "DELETE r, existing "
                + "CREATE (content)-[:" + HAS_LOCATION_RELATIONSHIP + "]->"
                + "(location:" + LOCATION + " { "
                + START_DATE_TIME + ": " + param(START_DATE_TIME) + ", "
                + END_DATE_TIME + ": " + param(END_DATE_TIME) + " "
                + " })");
    }

    public static LocationWriter create(Timer timer) {
        return new LocationWriter(timer);
    }

    public void write(Content content, StatementRunner runner) {
        Timer.Context time = timer.time();

        ImmutableSet<Location> locations = content.getManifestedAs()
                .stream()
                .flatMap(encoding -> encoding.getAvailableAt().stream())
                .filter(Objects::nonNull)
                .filter(Location::getAvailable)
                .filter(location -> location.getPolicy() != null)
                .collect(MoreCollectors.toImmutableSet());

        if (locations.isEmpty()) {
            write(
                    removeAllLocationsStatement.withParameters(ImmutableMap.of(
                            CONTENT_ID, content.getId().longValue()
                    )),
                    runner
            );
        } else {
            locations.stream()
                    .map(location -> addLocationStatement.withParameters(ImmutableMap.of(
                            CONTENT_ID, content.getId().longValue(),
                            START_DATE_TIME, getAvailabilityStart(location),
                            END_DATE_TIME, getAvailabilityEnd(location)
                    )))
                    .forEach(statement -> write(statement, runner));
        }

        time.stop();
    }

    private String getAvailabilityStart(Location location) {
        DateTime availabilityStart = location.getPolicy().getAvailabilityStart();

        if (availabilityStart != null) {
            return availabilityStart.toString();
        }

        // Missing availability start means it has always been available
        return AVAILABLE_FROM_FOREVER.toString();
    }

    private String getAvailabilityEnd(Location location) {
        DateTime availabilityEnd = location.getPolicy().getAvailabilityEnd();

        if (availabilityEnd != null) {
            return availabilityEnd.toString();
        }

        // Missing availability start means it will be available forever
        return AVAILABLE_UNTIL_FOREVER.toString();
    }
}
