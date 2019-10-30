package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.content.v2.model.Identified;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;
import org.joda.time.LocalDate;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    @Field(name = "noo") private Boolean newOneOff;
    @Field(name = "p") private Boolean premiere;
    @Field(name = "co") private Boolean continuation;
    @Field(name = "thd") private Boolean is3d;
    @Field(name = "br") private Boolean blackoutRestriction;
    @Field(name = "rr") private Boolean revisedRepeat;
    @Field(name = "cf") private Map<String, String> customFields;


    public Broadcast() {}

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

    public Boolean getNewOneOff() {
        return newOneOff;
    }

    public void setNewOneOff(Boolean newOneOff) {
        this.newOneOff = newOneOff;
    }

    public Boolean getPremiere() {
        return premiere;
    }

    public void setPremiere(Boolean premiere) {
        this.premiere = premiere;
    }

    public Boolean getContinuation() {
        return continuation;
    }

    public void setContinuation(Boolean continuation) {
        this.continuation = continuation;
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

    public Boolean getRevisedRepeat() {
        return revisedRepeat;
    }

    public void setRevisedRepeat(Boolean revisedRepeat) {
        this.revisedRepeat = revisedRepeat;
    }

    public Map<String, String> getCustomFields() {
        return customFields;
    }

    public void setCustomFields(Map<String, String> customFields) {
        this.customFields = customFields;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Broadcast broadcast = (Broadcast) object;
        return Objects.equals(id, broadcast.id) &&
                Objects.equals(canonicalUri, broadcast.canonicalUri) &&
                Objects.equals(curie, broadcast.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, broadcast.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, broadcast.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, broadcast.equivalentTo) &&
                Objects.equals(channelId, broadcast.channelId) &&
                Objects.equals(transmissionStart, broadcast.transmissionStart) &&
                Objects.equals(transmissionEnd, broadcast.transmissionEnd) &&
                Objects.equals(broadcastDuration, broadcast.broadcastDuration) &&
                Objects.equals(scheduleDate, broadcast.scheduleDate) &&
                Objects.equals(activelyPublished, broadcast.activelyPublished) &&
                Objects.equals(versionId, broadcast.versionId) &&
                Objects.equals(repeat, broadcast.repeat) &&
                Objects.equals(subtitled, broadcast.subtitled) &&
                Objects.equals(signed, broadcast.signed) &&
                Objects.equals(audioDescribed, broadcast.audioDescribed) &&
                Objects.equals(highDefinition, broadcast.highDefinition) &&
                Objects.equals(widescreen, broadcast.widescreen) &&
                Objects.equals(surround, broadcast.surround) &&
                Objects.equals(live, broadcast.live) &&
                Objects.equals(newSeries, broadcast.newSeries) &&
                Objects.equals(newEpisode, broadcast.newEpisode) &&
                Objects.equals(newOneOff, broadcast.newOneOff) &&
                Objects.equals(premiere, broadcast.premiere) &&
                Objects.equals(continuation, broadcast.continuation) &&
                Objects.equals(is3d, broadcast.is3d) &&
                Objects.equals(blackoutRestriction, broadcast.blackoutRestriction) &&
                Objects.equals(revisedRepeat, broadcast.revisedRepeat) &&
                NullOrEmptyEquality.equals(customFields, broadcast.customFields);
    }

    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(id, canonicalUri, curie, aliasUrls, aliases, equivalentTo, channelId, transmissionStart, transmissionEnd, broadcastDuration, scheduleDate, activelyPublished, versionId, repeat, subtitled, signed, audioDescribed, highDefinition, widescreen, surround, live, newSeries, newEpisode, newOneOff, premiere, continuation, is3d, blackoutRestriction, revisedRepeat, customFields);
    }
}
