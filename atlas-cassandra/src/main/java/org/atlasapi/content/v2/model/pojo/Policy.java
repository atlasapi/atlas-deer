package org.atlasapi.content.v2.model.pojo;

import java.util.List;
import java.util.Set;

import org.atlasapi.content.v2.model.Identified;
import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Ref;

import org.joda.time.Instant;

public class Policy implements Identified {

    private Long id;
    private String canonicalUri;
    private String curie;
    private Set<String> aliasUrls;
    private Set<Alias> aliases;
    private Set<Ref> equivalentTo;
    private Instant lastUpdated;
    private Instant equivalenceUpdate;
    private Instant availabilityStart;
    private Instant availabilityEnd;
    private Instant drmPlayableFrom;
    private Set<String> availableCountries;
    private Integer availabilityLength;
    private String revenueContract;
    private Set<String> subscriptionPackages;
    private Price price;
    private List<Pricing> pricing;
    private Long serviceId;
    private Long playerId;
    private String platform;
    private String network;
    private Instant actualAvailabilityStart;

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

    public Instant getAvailabilityStart() {
        return availabilityStart;
    }

    public void setAvailabilityStart(Instant availabilityStart) {
        this.availabilityStart = availabilityStart;
    }

    public Instant getAvailabilityEnd() {
        return availabilityEnd;
    }

    public void setAvailabilityEnd(Instant availabilityEnd) {
        this.availabilityEnd = availabilityEnd;
    }

    public Instant getDrmPlayableFrom() {
        return drmPlayableFrom;
    }

    public void setDrmPlayableFrom(Instant drmPlayableFrom) {
        this.drmPlayableFrom = drmPlayableFrom;
    }

    public Set<String> getAvailableCountries() {
        return availableCountries;
    }

    public void setAvailableCountries(Set<String> availableCountries) {
        this.availableCountries = availableCountries;
    }

    public Integer getAvailabilityLength() {
        return availabilityLength;
    }

    public void setAvailabilityLength(Integer availabilityLength) {
        this.availabilityLength = availabilityLength;
    }

    public String getRevenueContract() {
        return revenueContract;
    }

    public void setRevenueContract(String revenueContract) {
        this.revenueContract = revenueContract;
    }

    public Set<String> getSubscriptionPackages() {
        return subscriptionPackages;
    }

    public void setSubscriptionPackages(Set<String> subscriptionPackages) {
        this.subscriptionPackages = subscriptionPackages;
    }

    public Price getPrice() {
        return price;
    }

    public void setPrice(Price price) {
        this.price = price;
    }

    public List<Pricing> getPricing() {
        return pricing;
    }

    public void setPricing(List<Pricing> pricing) {
        this.pricing = pricing;
    }

    public Long getServiceId() {
        return serviceId;
    }

    public void setServiceId(Long serviceId) {
        this.serviceId = serviceId;
    }

    public Long getPlayerId() {
        return playerId;
    }

    public void setPlayerId(Long playerId) {
        this.playerId = playerId;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public Instant getActualAvailabilityStart() {
        return actualAvailabilityStart;
    }

    public void setActualAvailabilityStart(Instant actualAvailabilityStart) {
        this.actualAvailabilityStart = actualAvailabilityStart;
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
