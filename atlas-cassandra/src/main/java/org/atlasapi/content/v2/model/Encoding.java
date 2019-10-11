package org.atlasapi.content.v2.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.atlasapi.comparison.ExcludeFromObjectComparison;
import org.atlasapi.content.v2.model.pojo.Location;
import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Encoding implements Identified {

    private Long id;
    private String canonicalUri;
    private String curie;
    private Set<String> aliasUrls;
    private Set<Alias> aliases;
    private Set<Ref> equivalentTo;
    @ExcludeFromObjectComparison
    private Instant lastUpdated;
    @ExcludeFromObjectComparison
    private Instant equivalenceUpdate;
    private Set<Location> availableAt;
    private Boolean containsAdvertising;
    private Integer advertisingDuration;
    private Long duration;
    private Integer bitRate;
    private Integer audioBitRate;
    private Integer audioChannels;
    private String audioCoding;
    private String videoAspectRatio;
    private Integer videoBitRate;
    private String videoCoding;
    private Float videoFrameRate;
    private Integer videoHorizontalSize;
    private Boolean videoProgressiveScan;
    private Integer videoVerticalSize;
    private Long dataSize;
    private String dataContainerFormat;
    private String source;
    private String distributor;
    private Boolean hasDog;
    private Boolean is3d;
    private String quality;
    private String qualityDetail;
    private String versionId;
    private Map<String, String> customFields;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCanonicalUri() {
        return canonicalUri;
    }

    public void setCanonicalUri(String canonicalUri) {
        this.canonicalUri = canonicalUri;
    }

    public String getCurie() {
        return curie;
    }

    public void setCurie(String curie) {
        this.curie = curie;
    }

    public Set<String> getAliasUrls() {
        return aliasUrls;
    }

    public void setAliasUrls(Set<String> aliasUrls) {
        this.aliasUrls = aliasUrls;
    }

    public Set<Alias> getAliases() {
        return aliases;
    }

    public void setAliases(Set<Alias> aliases) {
        this.aliases = aliases;
    }

    public Set<Ref> getEquivalentTo() {
        return equivalentTo;
    }

    public void setEquivalentTo(Set<Ref> equivalentTo) {
        this.equivalentTo = equivalentTo;
    }

    public Set<Location> getAvailableAt() {
        return availableAt;
    }

    public void setAvailableAt(Set<Location> availableAt) {
        this.availableAt = availableAt;
    }

    public Boolean getContainsAdvertising() {
        return containsAdvertising;
    }

    public void setContainsAdvertising(Boolean containsAdvertising) {
        this.containsAdvertising = containsAdvertising;
    }

    public Integer getAdvertisingDuration() {
        return advertisingDuration;
    }

    public void setAdvertisingDuration(Integer advertisingDuration) {
        this.advertisingDuration = advertisingDuration;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Integer getBitRate() {
        return bitRate;
    }

    public void setBitRate(Integer bitRate) {
        this.bitRate = bitRate;
    }

    public Integer getAudioBitRate() {
        return audioBitRate;
    }

    public void setAudioBitRate(Integer audioBitRate) {
        this.audioBitRate = audioBitRate;
    }

    public Integer getAudioChannels() {
        return audioChannels;
    }

    public void setAudioChannels(Integer audioChannels) {
        this.audioChannels = audioChannels;
    }

    public String getAudioCoding() {
        return audioCoding;
    }

    public void setAudioCoding(String audioCoding) {
        this.audioCoding = audioCoding;
    }

    public String getVideoAspectRatio() {
        return videoAspectRatio;
    }

    public void setVideoAspectRatio(String videoAspectRatio) {
        this.videoAspectRatio = videoAspectRatio;
    }

    public Integer getVideoBitRate() {
        return videoBitRate;
    }

    public void setVideoBitRate(Integer videoBitRate) {
        this.videoBitRate = videoBitRate;
    }

    public String getVideoCoding() {
        return videoCoding;
    }

    public void setVideoCoding(String videoCoding) {
        this.videoCoding = videoCoding;
    }

    public Float getVideoFrameRate() {
        return videoFrameRate;
    }

    public void setVideoFrameRate(Float videoFrameRate) {
        this.videoFrameRate = videoFrameRate;
    }

    public Integer getVideoHorizontalSize() {
        return videoHorizontalSize;
    }

    public void setVideoHorizontalSize(Integer videoHorizontalSize) {
        this.videoHorizontalSize = videoHorizontalSize;
    }

    public Boolean getVideoProgressiveScan() {
        return videoProgressiveScan;
    }

    public void setVideoProgressiveScan(Boolean videoProgressiveScan) {
        this.videoProgressiveScan = videoProgressiveScan;
    }

    public Integer getVideoVerticalSize() {
        return videoVerticalSize;
    }

    public void setVideoVerticalSize(Integer videoVerticalSize) {
        this.videoVerticalSize = videoVerticalSize;
    }

    public Long getDataSize() {
        return dataSize;
    }

    public void setDataSize(Long dataSize) {
        this.dataSize = dataSize;
    }

    public String getDataContainerFormat() {
        return dataContainerFormat;
    }

    public void setDataContainerFormat(String dataContainerFormat) {
        this.dataContainerFormat = dataContainerFormat;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDistributor() {
        return distributor;
    }

    public void setDistributor(String distributor) {
        this.distributor = distributor;
    }

    public Boolean getHasDog() {
        return hasDog;
    }

    public void setHasDog(Boolean hasDog) {
        this.hasDog = hasDog;
    }

    public Boolean getIs3d() {
        return is3d;
    }

    public void setIs3d(Boolean is3d) {
        this.is3d = is3d;
    }

    public String getQuality() {
        return quality;
    }

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public String getQualityDetail() {
        return qualityDetail;
    }

    public void setQualityDetail(String qualityDetail) {
        this.qualityDetail = qualityDetail;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Instant getEquivalenceUpdate() {
        return equivalenceUpdate;
    }

    public void setEquivalenceUpdate(Instant equivalenceUpdate) {
        this.equivalenceUpdate = equivalenceUpdate;
    }

    public Map<String, String> getCustomFields() {
        return customFields;
    }

    public void setCustomFields(Map<String, String> customFields) {
        this.customFields = customFields;
    }

    public static class Wrapper {

        private Set<Encoding> encodings;

        @JsonCreator
        public Wrapper(
                @JsonProperty("encodings") Set<Encoding> encodings
        ) {
            this.encodings = encodings;
        }

        @JsonProperty("encodings")
        public Set<Encoding> getEncodings() {
            return encodings;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            Wrapper wrapper = (Wrapper) object;
            return NullOrEmptyEquality.equals(encodings, wrapper.encodings);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Encoding encoding = (Encoding) object;
        return Objects.equals(id, encoding.id) &&
                Objects.equals(canonicalUri, encoding.canonicalUri) &&
                Objects.equals(curie, encoding.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, encoding.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, encoding.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, encoding.equivalentTo) &&
                NullOrEmptyEquality.equals(availableAt, encoding.availableAt) &&
                Objects.equals(containsAdvertising, encoding.containsAdvertising) &&
                Objects.equals(advertisingDuration, encoding.advertisingDuration) &&
                Objects.equals(duration, encoding.duration) &&
                Objects.equals(bitRate, encoding.bitRate) &&
                Objects.equals(audioBitRate, encoding.audioBitRate) &&
                Objects.equals(audioChannels, encoding.audioChannels) &&
                Objects.equals(audioCoding, encoding.audioCoding) &&
                Objects.equals(videoAspectRatio, encoding.videoAspectRatio) &&
                Objects.equals(videoBitRate, encoding.videoBitRate) &&
                Objects.equals(videoCoding, encoding.videoCoding) &&
                Objects.equals(videoFrameRate, encoding.videoFrameRate) &&
                Objects.equals(videoHorizontalSize, encoding.videoHorizontalSize) &&
                Objects.equals(videoProgressiveScan, encoding.videoProgressiveScan) &&
                Objects.equals(videoVerticalSize, encoding.videoVerticalSize) &&
                Objects.equals(dataSize, encoding.dataSize) &&
                Objects.equals(dataContainerFormat, encoding.dataContainerFormat) &&
                Objects.equals(source, encoding.source) &&
                Objects.equals(distributor, encoding.distributor) &&
                Objects.equals(hasDog, encoding.hasDog) &&
                Objects.equals(is3d, encoding.is3d) &&
                Objects.equals(quality, encoding.quality) &&
                Objects.equals(qualityDetail, encoding.qualityDetail) &&
                Objects.equals(versionId, encoding.versionId) &&
                NullOrEmptyEquality.equals(customFields, encoding.customFields);
    }
}
