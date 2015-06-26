package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Encoding.Builder;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.media.MimeType;

public class EncodingSerializer {

    private final LocationSerializer locationSerializer = new LocationSerializer();

    public ContentProtos.Encoding.Builder serialize(Encoding encoding) {
        Builder builder = ContentProtos.Encoding.newBuilder();
        for (Location location : encoding.getAvailableAt()) {
            builder.addLocation(locationSerializer.serialize(location));
        }
        if (encoding.getAdvertisingDuration() != null) {
            builder.setAdvertisingDuration(encoding.getAdvertisingDuration());
        }
        if (encoding.getAudioBitRate() != null) {
            builder.setAudioBitRate(encoding.getAudioBitRate());
        }
        if (encoding.getAudioChannels() != null) {
            builder.setAudioChannels(encoding.getAudioChannels());
        }
        if (encoding.getAudioCoding() != null) {
            builder.setAudioCoding(encoding.getAudioCoding().toString());
        }
        if (encoding.getBitRate() != null) {
            builder.setBitRate(encoding.getBitRate());
        }
        if (encoding.getContainsAdvertising() != null) {
            builder.setContainsAdvertising(encoding.getContainsAdvertising());
        }
        if (encoding.getDataContainerFormat() != null) {
            builder.setDataContainerFormat(encoding.getDataContainerFormat().toString());
        }
        if (encoding.getDataSize() != null) {
            builder.setDataSize(encoding.getDataSize());
        }
        
        if (encoding.getDuration() != null) {
            builder.setDuration(encoding.getDuration().getStandardSeconds());
        }
        
        if (encoding.getDistributor() != null) {
            builder.setDistributor(encoding.getDistributor());
        }
        if (encoding.getHasDOG() != null) {
            builder.setHasDog(encoding.getHasDOG());
        }
        if (encoding.getSource() != null) {
            builder.setSource(encoding.getSource());
        }
        if (encoding.getVideoAspectRatio() != null) {
            builder.setVideoAspectRatio(encoding.getVideoAspectRatio());
        }
        if (encoding.getVideoBitRate() != null) {
            builder.setVideoBitRate(encoding.getVideoBitRate());
        }
        if (encoding.getVideoCoding() != null) {
            builder.setVideoCoding(encoding.getVideoCoding().toString());
        }
        if (encoding.getVideoFrameRate() != null) {
            builder.setVideoFrameRate(encoding.getVideoFrameRate());
        }
        if (encoding.getVideoHorizontalSize() != null) {
            builder.setVideoHorizontalSize(encoding.getVideoHorizontalSize());
        }
        if (encoding.getVideoProgressiveScan() != null) {
            builder.setVideoProgressiveScan(encoding.getVideoProgressiveScan());
        }
        if (encoding.getVideoVerticalSize() != null) {
            builder.setVideoVerticalSize(encoding.getVideoVerticalSize());
        }
        if (encoding.is3d() != null) {
            builder.setIsThreeD(encoding.is3d());
        }
        if (encoding.getVersionId() != null) {
            builder.setVersion(encoding.getVersionId());
        }
        if (encoding.getQuality() != null) {
            builder.setQuality(encoding.getQuality().toString());
        }
        if (encoding.getQualityDetail() != null) {
            builder.setQualityDetail(encoding.getQualityDetail());
        }
        return builder;
    }

    public Encoding deserialize(ContentProtos.Encoding msg) {
        Encoding encoding = new Encoding();
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        for (ContentProtos.Location location : msg.getLocationList()) {
            locations.add(locationSerializer.deserialize(location));
        }
        encoding.setAvailableAt(locations.build());
        encoding.setAdvertisingDuration(msg.hasAdvertisingDuration() ? msg.getAdvertisingDuration()
                                                                    : null);
        encoding.setAudioBitRate(msg.hasAudioBitRate() ? msg.getAudioBitRate() : null);
        encoding.setAudioChannels(msg.hasAudioChannels() ? msg.getAudioChannels() : null);
        if (msg.hasAudioCoding()) {
            encoding.setAudioCoding(MimeType.fromString(msg.getAudioCoding()));
        }
        encoding.setBitRate(msg.hasBitRate() ? msg.getBitRate() : null);
        encoding.setContainsAdvertising(msg.hasContainsAdvertising() ? msg.getContainsAdvertising()
                                                                    : null);
        if (msg.hasDataContainerFormat()) {
            encoding.setDataContainerFormat(MimeType.fromString(msg.getDataContainerFormat()));
        }
        encoding.setDataSize(msg.hasDataSize() ? msg.getDataSize() : null);
        encoding.setDistributor(msg.hasDistributor() ? msg.getDistributor() : null);
        encoding.setHasDOG(msg.hasHasDog() ? msg.getHasDog() : null);
        encoding.setSource(msg.hasSource() ? msg.getSource() : null);
        encoding.setVideoAspectRatio(msg.hasVideoAspectRatio() ? msg.getVideoAspectRatio() : null);
        encoding.setVideoBitRate(msg.hasVideoBitRate() ? msg.getVideoBitRate() : null);
        if (msg.hasVideoCoding()) {
            encoding.setVideoCoding(MimeType.fromString(msg.getVideoCoding()));
        }
        encoding.setVideoFrameRate(msg.hasVideoFrameRate() ? msg.getVideoFrameRate() : null);
        encoding.setVideoHorizontalSize(msg.hasVideoHorizontalSize() ? msg.getVideoHorizontalSize()
                                                                    : null);
        encoding.setVideoProgressiveScan(msg.hasVideoProgressiveScan() ? msg.getVideoProgressiveScan()
                                                                      : null);
        encoding.setVideoVerticalSize(msg.hasVideoVerticalSize() ? msg.getVideoVerticalSize()
                                                                : null);
        encoding.set3d(msg.hasIsThreeD() ? msg.getIsThreeD() : null);
        encoding.setVersionId(msg.hasVersion() ? msg.getVersion() : null);
        encoding.setQuality(msg.hasQuality() ? Quality.valueOf(msg.getQuality()) : null);
        encoding.setQualityDetail(msg.hasQualityDetail() ? msg.getQualityDetail() : null);
        return encoding;
    }

}
