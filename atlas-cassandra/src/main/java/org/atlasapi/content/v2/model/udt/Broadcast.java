package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import org.atlasapi.content.v2.model.Identified;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;
import org.joda.time.LocalDate;

@UDT(name = "broadcast")
public class Broadcast implements Identified {

    @Field(name = "i") private Long id;
    @Field(name = "c") private String canonicalUri;
    @Field(name = "cu") private String curie;
    @Field(name = "au") private Set<String> aliasUrls;
    @Field(name = "a") private Set<Alias> aliases;
    @Field(name = "e") private Set<Ref> equivalentTo;
    @Field(name = "lu") private Instant lastUpdated;
    @Field(name = "eu") private Instant equivalenceUpdate;

    @Field(name = "ci") private Long channelId;
    @Field(name = "ts") private Instant transmissionStart;
    @Field(name = "te") private Instant transmissionEnd;
    @Field(name = "bd") private Long broadcastDuration;
    @Field(name = "sd") private LocalDate scheduleDate;
    @Field(name = "ap") private Boolean activelyPublished;
    @Field(name = "si") private String sourceId;
    @Field(name = "vi") private String versionId;
    @Field(name = "r") private Boolean repeat;
    @Field(name = "sub") private Boolean subtitled;
    @Field(name = "sgn") private Boolean signed;
    @Field(name = "ad") private Boolean audioDescribed;
    @Field(name = "hd") private Boolean highDefinition;
    @Field(name = "w") private Boolean widescreen;
    @Field(name = "sr") private Boolean surround;
    @Field(name = "l") private Boolean live;
    @Field(name = "ns") private Boolean newSeries;
    @Field(name = "ne") private Boolean newEpisode;
    @Field(name = "p") private Boolean premiere;
    @Field(name = "thd") private Boolean is3d;
    // TODO: fix this
    @Field(name = "br") private Boolean blackoutRestriction;

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

    public Long getChannelId() {
        return channelId;
    }

    public void setChannelId(Long channelId) {
        this.channelId = channelId;
    }

    public Instant getTransmissionStart() {
        return transmissionStart;
    }

    public void setTransmissionStart(Instant transmissionStart) {
        this.transmissionStart = transmissionStart;
    }

    public Instant getTransmissionEnd() {
        return transmissionEnd;
    }

    public void setTransmissionEnd(Instant transmissionEnd) {
        this.transmissionEnd = transmissionEnd;
    }

    public Long getBroadcastDuration() {
        return broadcastDuration;
    }

    public void setBroadcastDuration(Long broadcastDuration) {
        this.broadcastDuration = broadcastDuration;
    }

    public LocalDate getScheduleDate() {
        return scheduleDate;
    }

    public void setScheduleDate(LocalDate scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    public Boolean getActivelyPublished() {
        return activelyPublished;
    }

    public void setActivelyPublished(Boolean activelyPublished) {
        this.activelyPublished = activelyPublished;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public Boolean getRepeat() {
        return repeat;
    }

    public void setRepeat(Boolean repeat) {
        this.repeat = repeat;
    }

    public Boolean getSubtitled() {
        return subtitled;
    }

    public void setSubtitled(Boolean subtitled) {
        this.subtitled = subtitled;
    }

    public Boolean getSigned() {
        return signed;
    }

    public void setSigned(Boolean signed) {
        this.signed = signed;
    }

    public Boolean getAudioDescribed() {
        return audioDescribed;
    }

    public void setAudioDescribed(Boolean audioDescribed) {
        this.audioDescribed = audioDescribed;
    }

    public Boolean getHighDefinition() {
        return highDefinition;
    }

    public void setHighDefinition(Boolean highDefinition) {
        this.highDefinition = highDefinition;
    }

    public Boolean getWidescreen() {
        return widescreen;
    }

    public void setWidescreen(Boolean widescreen) {
        this.widescreen = widescreen;
    }

    public Boolean getSurround() {
        return surround;
    }

    public void setSurround(Boolean surround) {
        this.surround = surround;
    }

    public Boolean getLive() {
        return live;
    }

    public void setLive(Boolean live) {
        this.live = live;
    }

    public Boolean getNewSeries() {
        return newSeries;
    }

    public void setNewSeries(Boolean newSeries) {
        this.newSeries = newSeries;
    }

    public Boolean getNewEpisode() {
        return newEpisode;
    }

    public void setNewEpisode(Boolean newEpisode) {
        this.newEpisode = newEpisode;
    }

    public Boolean getPremiere() {
        return premiere;
    }

    public void setPremiere(Boolean premiere) {
        this.premiere = premiere;
    }

    public Boolean getIs3d() {
        return is3d;
    }

    public void setIs3d(Boolean is3d) {
        this.is3d = is3d;
    }

    public Boolean getBlackoutRestriction() {
        return blackoutRestriction;
    }

    public void setBlackoutRestriction(Boolean blackoutRestriction) {
        this.blackoutRestriction = blackoutRestriction;
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
