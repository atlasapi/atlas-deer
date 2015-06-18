package org.atlasapi.output.annotation;


import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.atlasapi.content.Content;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Item;
import org.atlasapi.content.Location;
import org.atlasapi.content.Player;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Service;
import org.atlasapi.output.AnnotationRegistry;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;
import org.atlasapi.output.writers.PlayerWriter;
import org.atlasapi.output.writers.ServiceWriter;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.atlasapi.system.legacy.LegacyPlayerTransformer;
import org.atlasapi.system.legacy.LegacyServiceTransformer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;


public class LocationsAnnotation extends OutputAnnotation<Content> {

    private final EncodedLocationWriter encodedLocationWriter;

    public LocationsAnnotation(PlayerResolver playerResolver, ServiceResolver serviceResolver) {
        this.encodedLocationWriter = new EncodedLocationWriter(
                "locations", playerResolver, serviceResolver
        );
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            writer.writeList(encodedLocationWriter, encodedLocations(item), ctxt);
        }
    }

    private Iterable<EncodedLocation> encodedLocations(Item item) {
        return encodedLocations(item.getManifestedAs());
    }

    private Iterable<EncodedLocation> encodedLocations(Set<Encoding> manifestedAs) {
        return Iterables.concat(Iterables.transform(manifestedAs,
                encoding -> {
                    Builder<EncodedLocation> builder = ImmutableList.builder();
                    for (Location location : encoding.getAvailableAt()) {
                        builder.add(new EncodedLocation(encoding, location));
                    }
                    return builder.build();
                }
        ));
    }

    public static final class EncodedLocationWriter implements EntityListWriter<EncodedLocation> {

        private final static Logger log = LoggerFactory.getLogger(EncodedLocation.class);

        private final AliasWriter aliasWriter;
        private final PlayerResolver playerResolver;
        private final EntityWriter<Player> playerWriter = new PlayerWriter();
        private final EntityWriter<Service> serviceWriter = new ServiceWriter();
        private final LegacyPlayerTransformer playerTransformer = new LegacyPlayerTransformer();
        private final LegacyServiceTransformer serviceTransformer = new LegacyServiceTransformer();
        private final ServiceResolver serviceResolver;
        private final String listName;

        public EncodedLocationWriter(String listName, PlayerResolver playerResolver, ServiceResolver serviceResolver) {
            this.listName = checkNotNull(listName);
            this.serviceResolver = checkNotNull(serviceResolver);
            this.playerResolver = checkNotNull(playerResolver);
            this.aliasWriter = new AliasWriter();
        }

        private Boolean isAvailable(Policy input) {
            return (input.getAvailabilityStart() == null || !(new DateTime(input.getAvailabilityStart()).isAfterNow()))
                    && (input.getAvailabilityEnd() == null || new DateTime(input.getAvailabilityEnd()).isAfterNow());
        }

        @Override
        public void write(EncodedLocation entity, FieldWriter writer, OutputContext ctxt)
                throws IOException {
            writeLocation(entity, writer, ctxt);
        }

        private void writeLocation(EncodedLocation entity, FieldWriter writer, OutputContext ctxt) throws IOException {
            Encoding encoding = entity.getEncoding();
            Location location = entity.getLocation();
            Policy policy = location.getPolicy();

            writer.writeField("uri", location.getUri());
            writer.writeList(aliasWriter, location.getAliases(), ctxt);
            writer.writeField("available", isAvailable(policy));
            writer.writeField("transport_is_live", location.getTransportIsLive());
            writer.writeField("transport_type", location.getTransportType());
            writer.writeField("transport_sub_type", location.getTransportSubType());
            writer.writeField("embed_id", location.getEmbedId());
            writer.writeField("embed_code", location.getEmbedCode());

            writer.writeField("availability_start", policy.getAvailabilityStart());
            writer.writeField("availability_end", policy.getAvailabilityEnd());
            writer.writeList("available_countries", "country", policy.getAvailableCountries(), ctxt);
            if (policy.getServiceRef() != null) {
                writeService(writer, ctxt, policy);
            }
            if (policy.getPlayerRef() != null) {
                writePlayer(writer, ctxt, policy);
            }
            writer.writeField("drm_playable_from", policy.getDrmPlayableFrom());
            if (policy.getPrice() != null) {
                writer.writeField("currency", policy.getPrice().getCurrency());
                writer.writeField("price", policy.getPrice().getAmount());
            }
            writer.writeField("revenue_contract", policy.getRevenueContract());
            writer.writeList("subscription_packages", "subscription_package",
                    policy.getSubscriptionPackages(), ctxt);

            writer.writeField("data_container_format", encoding.getDataContainerFormat());
            writer.writeField("data_size", encoding.getDataSize());
            writer.writeField("distributor", encoding.getDistributor());
            writer.writeField("has_dog", encoding.getHasDOG());
            writer.writeField("advertising_duration", encoding.getAdvertisingDuration());
            writer.writeField("contains_advertising", encoding.getContainsAdvertising());
            writer.writeField("source", encoding.getSource());
            writer.writeField("quality", encoding.getQuality());
            writer.writeField("quality_detail", encoding.getQualityDetail());
            writer.writeField("bit_rate", encoding.getBitRate());
            writer.writeField("audio_bit_rate", encoding.getAudioBitRate());
            writer.writeField("audio_channels", encoding.getAudioChannels());
            writer.writeField("audio_coding", encoding.getAudioCoding());
            writer.writeField("video_aspect_ratio", encoding.getVideoAspectRatio());
            writer.writeField("video_bit_rate", encoding.getVideoBitRate());
            writer.writeField("video_coding", encoding.getVideoCoding());
            writer.writeField("video_frame_rate", encoding.getVideoFrameRate());
            writer.writeField("video_horizontal_size", encoding.getVideoHorizontalSize());
            writer.writeField("video_progressive_scan", encoding.getVideoProgressiveScan());
            writer.writeField("video_vertical_size", encoding.getVideoVerticalSize());
        }

        private void writePlayer(FieldWriter writer, OutputContext ctxt, Policy policy) throws IOException {
            Optional<org.atlasapi.media.entity.Player> maybePlayer
                    = playerResolver.playerFor(policy.getPlayerRef().longValue());
            if (maybePlayer.isPresent()) {
                Player player = playerTransformer.apply(maybePlayer.get());
                writer.writeObject(playerWriter, player, ctxt);
            } else {
                log.warn("Failed to resolve Player {}", policy.getPlayerRef());
            }
        }

        private void writeService(FieldWriter writer, OutputContext ctxt, Policy policy) throws IOException {
            Optional<org.atlasapi.media.entity.Service> maybeService =
                    serviceResolver.serviceFor(policy.getServiceRef().longValue());
            if (maybeService.isPresent()) {
                Service service = serviceTransformer.apply(maybeService.get());
                writer.writeObject(serviceWriter, service, ctxt);
            } else {
                log.warn("Failed to resolve Service {}", policy.getServiceRef());
            }
        }

        @Override
        public String fieldName(EncodedLocation entity) {
            return "location";
        }

        @Override
        public String listName() {
            return listName;
        }
    }
}
