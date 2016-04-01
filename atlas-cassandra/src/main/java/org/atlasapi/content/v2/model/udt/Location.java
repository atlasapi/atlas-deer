package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import org.atlasapi.content.v2.model.Identified;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "location")
public class Location implements Identified {

    @Field(name = "i") private Long id;
    @Field(name = "c") private String canonicalUri;
    @Field(name = "cu") private String curie;
    @Field(name = "au") private Set<String> aliasUrls;
    @Field(name = "a") private Set<Alias> aliases;
    @Field(name = "e") private Set<Ref> equivalentTo;
    @Field(name = "lu") private Instant lastUpdated;
    @Field(name = "eu") private Instant equivalenceUpdate;

    @Field(name = "av") private Boolean available;
    @Field(name = "til") private Boolean transportIsLive;
    @Field(name = "tst") private String transportSubType;
    @Field(name = "tt") private String transportType;
    @Field(name = "u") private String uri;
    @Field(name = "ec") private String embedCode;
    @Field(name = "ei") private String embedId;
    @Field(name = "p") private Policy policy;

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
