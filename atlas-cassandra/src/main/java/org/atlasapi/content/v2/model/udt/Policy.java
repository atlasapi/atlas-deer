package org.atlasapi.content.v2.model.udt;

import java.util.List;
import java.util.Set;

import org.atlasapi.content.v2.model.Identified;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "policy")
public class Policy implements Identified {

    @Field(name = "id") private Long id;
    @Field(name = "canonical_uri") private String canonicalUri;
    @Field(name = "curie") private String curie;
    @Field(name = "alias_urls") private Set<String> aliasUrls;
    @Field(name = "aliases") private Set<Alias> aliases;
    @Field(name = "equiv_to") private Set<Ref> equivalentTo;
    @Field(name = "last_updated") private Instant lastUpdated;
    @Field(name = "equiv_update") private Instant equivalenceUpdate;

    @Field(name = "avail_start") private Instant availabilityStart;
    @Field(name = "avail_end") private Instant availabilityEnd;
    @Field(name = "drm_playable_from") private Instant drmPlayableFrom;
    @Field(name = "avail_countries") private Set<String> availableCountries;
    @Field(name = "avail_length") private Integer availabilityLength;
    @Field(name = "revenue_contract") private String revenueContract;
    @Field(name = "subscr_packages") private Set<String> subscriptionPackages;
    @Field(name = "price") private Price price;
    @Field(name = "pricing") private List<Pricing> pricing;
    @Field(name = "service_id") private Long serviceId;
    @Field(name = "player_id") private Long playerId;
    @Field(name = "platform") private String platform;
    @Field(name = "network") private String network;
    @Field(name = "actual_avail_start") private Instant actualAvailabilityStart;

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
