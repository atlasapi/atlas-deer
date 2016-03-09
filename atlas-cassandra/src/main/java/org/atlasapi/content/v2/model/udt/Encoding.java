package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "encoding")
public class Encoding {

    @Field(name = "i") private Long id;
    @Field(name = "c") private String canonicalUri;
    @Field(name = "cu") private String curie;
    @Field(name = "au") private Set<String> aliasUrls;
    @Field(name = "a") private Set<Alias> aliases;
    @Field(name = "e") private Set<Ref> equivalentTo;
    @Field(name = "lu") private Instant lastUpdated;
    @Field(name = "eu") private Instant equivalenceUpdate;

    @Field(name = "aa") private Set<Location> availableAt;
    @Field(name = "ca") private Boolean containsAdvertising;
    @Field(name = "ad") private Integer advertisingDuration;
    @Field(name = "d") private Long duration;
    @Field(name = "br") private Integer bitRate;

    @Field(name = "abr") private Integer audioBitRate;
    @Field(name = "ac") private Integer audioChannels;
    @Field(name = "acd") private String audioCoding;

    @Field(name = "var") private String videoAspectRatio;
    @Field(name = "vbr") private Integer videoBitRate;
    @Field(name = "vc") private String videoCoding;
    @Field(name = "vfr") private Float videoFrameRate;
    @Field(name = "vhs") private Integer videoHorizontalSize;
    @Field(name = "vps") private Boolean videoProgressiveScan;
    @Field(name = "vvs") private Integer videoVerticalSize;

    @Field(name = "ds") private Long dataSize;
    @Field(name = "dcf") private String dataContainerFormat;
    @Field(name = "src") private String source;
    @Field(name = "dst") private String distributor;
    @Field(name = "hd") private Boolean hasDog;
    @Field(name = "thd") private Boolean is3d;
    @Field(name = "q") private String quality;
    @Field(name = "qd") private String qualityDetail;
    @Field(name = "vi") private String versionId;

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
}
