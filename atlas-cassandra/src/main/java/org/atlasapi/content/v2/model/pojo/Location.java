package org.atlasapi.content.v2.model.pojo;

import java.util.Set;

import org.atlasapi.content.v2.model.Identified;
import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Ref;

import org.joda.time.Instant;

public class Location implements Identified {

    private Long id;
    private String canonicalUri;
    private String curie;
    private Set<String> aliasUrls;
    private Set<Alias> aliases;
    private Set<Ref> equivalentTo;
    private Instant lastUpdated;
    private Instant equivalenceUpdate;
    private Boolean available;
    private Boolean transportIsLive;
    private String transportSubType;
    private String transportType;
    private String uri;
    private String embedCode;
    private String embedId;
    private Policy policy;

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

    public Boolean getAvailable() {
        return available;
    }

    public void setAvailable(Boolean available) {
        this.available = available;
    }

    public Boolean getTransportIsLive() {
        return transportIsLive;
    }

    public void setTransportIsLive(Boolean transportIsLive) {
        this.transportIsLive = transportIsLive;
    }

    public String getTransportSubType() {
        return transportSubType;
    }

    public void setTransportSubType(String transportSubType) {
        this.transportSubType = transportSubType;
    }

    public String getTransportType() {
        return transportType;
    }

    public void setTransportType(String transportType) {
        this.transportType = transportType;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getEmbedCode() {
        return embedCode;
    }

    public void setEmbedCode(String embedCode) {
        this.embedCode = embedCode;
    }

    public String getEmbedId() {
        return embedId;
    }

    public void setEmbedId(String embedId) {
        this.embedId = embedId;
    }

    public Policy getPolicy() {
        return policy;
    }

    public void setPolicy(Policy policy) {
        this.policy = policy;
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
