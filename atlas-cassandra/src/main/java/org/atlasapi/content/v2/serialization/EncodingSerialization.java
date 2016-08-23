package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Quality;
import org.atlasapi.content.v2.model.Encoding;
import org.atlasapi.content.v2.model.udt.Location;
import org.atlasapi.content.v2.serialization.setters.IdentifiedSetter;

import com.metabroadcast.common.media.MimeType;

import org.joda.time.Duration;

public class EncodingSerialization {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();
    private final LocationSerialization location = new LocationSerialization();

    public Encoding serialize(org.atlasapi.content.Encoding encoding) {
        if (encoding == null) {
            return null;
        }
        Encoding internal =
                new Encoding();

        identifiedSetter.serialize(internal, encoding);

        internal.setAvailableAt(encoding.getAvailableAt()
                .stream()
                .map(location::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setContainsAdvertising(encoding.getContainsAdvertising());
        internal.setAdvertisingDuration(encoding.getAdvertisingDuration());
        Duration duration = encoding.getDuration();
        if (duration != null) {
            internal.setDuration(duration.getMillis());
        }
        internal.setBitRate(encoding.getBitRate());

        internal.setAudioBitRate(encoding.getAudioBitRate());
        internal.setAudioChannels(encoding.getAudioChannels());
        MimeType audioCoding = encoding.getAudioCoding();
        if (audioCoding != null) {
            internal.setAudioCoding(audioCoding.name());
        }

        internal.setVideoAspectRatio(encoding.getVideoAspectRatio());
        internal.setVideoBitRate(encoding.getVideoBitRate());
        MimeType videoCoding = encoding.getVideoCoding();
        if (videoCoding != null) {
            internal.setVideoCoding(videoCoding.name());
        }
        internal.setVideoFrameRate(encoding.getVideoFrameRate());
        internal.setVideoHorizontalSize(encoding.getVideoHorizontalSize());
        internal.setVideoProgressiveScan(encoding.getVideoProgressiveScan());
        internal.setVideoVerticalSize(encoding.getVideoVerticalSize());
        internal.setDataSize(encoding.getDataSize());
        MimeType dataContainerFormat = encoding.getDataContainerFormat();
        if (dataContainerFormat != null) {
            internal.setDataContainerFormat(dataContainerFormat.name());
        }
        internal.setSource(encoding.getSource());
        internal.setDistributor(encoding.getDistributor());
        internal.setHasDog(encoding.getHasDOG());
        internal.setIs3d(encoding.is3d());
        Quality quality = encoding.getQuality();
        if (quality != null) {
            internal.setQuality(quality.name());
        }
        internal.setQualityDetail(encoding.getQualityDetail());
        internal.setVersionId(encoding.getVersionId());

        return internal;
    }

    public org.atlasapi.content.Encoding deserialize(Encoding internal) {
        org.atlasapi.content.Encoding encoding = new org.atlasapi.content.Encoding();

        identifiedSetter.deserialize(encoding, internal);

        Set<Location> availableAt = internal.getAvailableAt();
        if (availableAt != null) {
            encoding.setAvailableAt(availableAt.stream()
                    .map(location::deserialize)
                    .collect(Collectors.toSet()));
        }

        encoding.setContainsAdvertising(internal.getContainsAdvertising());
        encoding.setAdvertisingDuration(internal.getAdvertisingDuration());
        encoding.setDuration(new Duration(internal.getDuration()));
        encoding.setBitRate(internal.getBitRate());
        encoding.setAudioBitRate(internal.getAudioBitRate());
        encoding.setAudioChannels(internal.getAudioChannels());

        String audioCoding = internal.getAudioCoding();
        if (audioCoding != null) {
            encoding.setAudioCoding(MimeType.valueOf(audioCoding));
        }

        encoding.setVideoAspectRatio(internal.getVideoAspectRatio());

        encoding.setVideoBitRate(internal.getVideoBitRate());

        String videoCoding = internal.getVideoCoding();
        if (videoCoding != null) {
            encoding.setVideoCoding(MimeType.valueOf(videoCoding));
        }

        encoding.setVideoFrameRate(internal.getVideoFrameRate());
        encoding.setVideoHorizontalSize(internal.getVideoHorizontalSize());
        encoding.setVideoVerticalSize(internal.getVideoVerticalSize());
        encoding.setVideoProgressiveScan(internal.getVideoProgressiveScan());

        encoding.setDataSize(internal.getDataSize());

        String dataContainerFormat = internal.getDataContainerFormat();
        if (dataContainerFormat != null) {
            encoding.setDataContainerFormat(MimeType.valueOf(dataContainerFormat));
        }

        encoding.setSource(internal.getSource());
        encoding.setDistributor(internal.getDistributor());
        encoding.setHasDOG(internal.getHasDog());
        encoding.set3d(internal.getIs3d());

        String quality = internal.getQuality();
        if (quality != null) {
            encoding.setQuality(Quality.valueOf(quality));
        }

        encoding.setQualityDetail(internal.getQualityDetail());

        encoding.setVersionId(internal.getVersionId());

        return encoding;
    }
}
