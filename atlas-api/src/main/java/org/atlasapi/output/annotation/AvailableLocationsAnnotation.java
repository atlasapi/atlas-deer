package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.content.Content;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Location;
import org.atlasapi.content.Policy;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.LocationsAnnotation.EncodedLocationWriter;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

public class AvailableLocationsAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    private final EncodedLocationWriter encodedLocationWriter;

    public AvailableLocationsAnnotation(PlayerResolver playerResolver,
            ServiceResolver serviceResolver) {
        this.encodedLocationWriter = new EncodedLocationWriter(
                "available_locations", playerResolver, serviceResolver
        );
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(encodedLocationWriter, encodedLocations(entity.getContent()), ctxt);
    }

    private Boolean isAvailable(Policy input) {
        return (input.getAvailabilityStart() == null || !(new DateTime(input.getAvailabilityStart())
                .isAfterNow()))
                && (input.getAvailabilityEnd() == null
                || new DateTime(input.getAvailabilityEnd()).isAfterNow());
    }

    private Iterable<EncodedLocation> encodedLocations(Content content) {
        return encodedLocations(content.getManifestedAs());
    }

    private Iterable<EncodedLocation> encodedLocations(Set<Encoding> manifestedAs) {
        return Iterables.concat(Iterables.transform(
                manifestedAs,
                encoding -> {
                    Builder<EncodedLocation> builder = ImmutableList.builder();
                    for (Location location : encoding.getAvailableAt()) {
                        if (isAvailable(location.getPolicy())) {
                            builder.add(new EncodedLocation(encoding, location));
                        }
                    }
                    return builder.build();
                }
        ));
    }
}
