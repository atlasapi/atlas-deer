package org.atlasapi.output.annotation;


import java.io.IOException;
import java.util.Set;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Version;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;


public class LocationsAnnotation extends OutputAnnotation<Content> {

    private final EncodedLocationWriter encodedLocationWriter;

    public static final class EncodedLocationWriter implements EntityListWriter<EncodedLocation> {

        private String listName;

        public EncodedLocationWriter(String listName) {
            this.listName = listName;
        }

        private Boolean isAvailable(Policy input) {
            return (input.getAvailabilityStart() == null || ! (new DateTime(input.getAvailabilityStart()).isAfterNow()))
                && (input.getAvailabilityEnd() == null || new DateTime(input.getAvailabilityEnd()).isAfterNow());
        }

        @Override
        public void write(EncodedLocation entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
            Encoding encoding = entity.getEncoding();
            Location location = entity.getLocation();
            Policy policy = location.getPolicy();
            
            writer.writeField("uri", location.getUri());
            writer.writeField("available", isAvailable(policy));
            writer.writeField("transport_is_live", location.getTransportIsLive());
            writer.writeField("transport_type", location.getTransportType());
            writer.writeField("transport_sub_type", location.getTransportSubType());
            writer.writeField("embed_id", location.getEmbedId());
            writer.writeField("embed_code", location.getEmbedCode());

            writer.writeField("availability_start", policy.getAvailabilityStart());
            writer.writeField("availability_end", policy.getAvailabilityEnd());
            writer.writeList("available_countries", "country", policy.getAvailableCountries(), ctxt);
            writer.writeField("platform", policy.getPlatform());
            writer.writeField("drm_playable_from", policy.getDrmPlayableFrom());
            if (policy.getPrice() != null) {
                writer.writeField("currency", policy.getPrice().getCurrency());
                writer.writeField("price", policy.getPrice().getAmount());
            }
            writer.writeField("revenue_contract", policy.getRevenueContract());

            writer.writeField("data_container_format", encoding.getDataContainerFormat());
            writer.writeField("data_size", encoding.getDataSize());
            writer.writeField("distributor", encoding.getDistributor());
            writer.writeField("has_dog", encoding.getHasDOG());
            writer.writeField("advertising_duration", encoding.getAdvertisingDuration());
            writer.writeField("contains_advertising", encoding.getContainsAdvertising());
            writer.writeField("source", encoding.getSource());
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

        @Override
        public String fieldName(EncodedLocation entity) {
            return "location";
        }

        @Override
        public String listName() {
            return listName;
        }
    }

    public LocationsAnnotation() {
        super();
        this.encodedLocationWriter = new EncodedLocationWriter("locations");
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            Item item = (Item) entity;
            writer.writeList(encodedLocationWriter, encodedLocations(item), ctxt);
        }
    }
    

    private Iterable<EncodedLocation> encodedLocations(Item item) {
        return Iterables.concat(Iterables.transform(item.getVersions(),
            new Function<Version, Iterable<EncodedLocation>>() {
                @Override
                public Iterable<EncodedLocation> apply(Version input) {
                    return encodedLocations(input.getManifestedAs());
                }
            }
        ));
    }

    private Iterable<EncodedLocation> encodedLocations(Set<Encoding> manifestedAs) {
        return Iterables.concat(Iterables.transform(manifestedAs,
            new Function<Encoding, Iterable<EncodedLocation>>() {
                @Override
                public Iterable<EncodedLocation> apply(Encoding encoding) {
                    Builder<EncodedLocation> builder = ImmutableList.builder();
                    for (Location location : encoding.getAvailableAt()) {
                        builder.add(new EncodedLocation(encoding, location));
                    }
                    return builder.build();
                }
            }
        ));
    }
}
