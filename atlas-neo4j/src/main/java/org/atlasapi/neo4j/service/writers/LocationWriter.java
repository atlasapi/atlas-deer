package org.atlasapi.neo4j.service.writers;

import java.util.Objects;

import org.atlasapi.content.Content;
import org.atlasapi.content.Location;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementRunner;

import static org.atlasapi.neo4j.service.model.Neo4jContent.CONTENT_ID;

public class LocationWriter extends Neo4jWriter {

    public static final DateTime AVAILABLE_FROM_FOREVER =
            new DateTime(1900, 1, 1, 0, 0, DateTimeZone.UTC);
    public static final DateTime AVAILABLE_UNTIL_FOREVER =
            new DateTime(3000, 1, 1, 0, 0, DateTimeZone.UTC);

    private static final String START_DATE_TIME = "startDateTime";
    private static final String END_DATE_TIME = "endDateTime";

    private final Statement addLocationStatement;

    private LocationWriter() {
        addLocationStatement = new Statement(""
                + "MATCH (content { " + CONTENT_ID + ": " + parameter(CONTENT_ID) + " }) "
                + "OPTIONAL MATCH (content)-[r:HAS_LOCATION]->(existing:Location) "
                + "DELETE r, existing "
                + "CREATE (content)-[:HAS_LOCATION]->(location:Location { "
                + START_DATE_TIME + ": " + parameter(START_DATE_TIME) + ", "
                + END_DATE_TIME + ": " + parameter(END_DATE_TIME) + " "
                + " })");
    }

    public static LocationWriter create() {
        return new LocationWriter();
    }

    public void write(Content content, StatementRunner runner) {
        ImmutableSet<Location> locations = content.getManifestedAs()
                .stream()
                .flatMap(encoding -> encoding.getAvailableAt().stream())
                .filter(Objects::nonNull)
                .filter(Location::getAvailable)
                .filter(location -> location.getPolicy() != null)
                .collect(MoreCollectors.toImmutableSet());

        locations.stream()
                .map(location -> addLocationStatement.withParameters(ImmutableMap.of(
                        CONTENT_ID, content.getId().longValue(),
                        START_DATE_TIME, getAvailabilityStart(location),
                        END_DATE_TIME, getAvailabilityEnd(location)
                )))
                .forEach(statement -> write(statement, runner));
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
