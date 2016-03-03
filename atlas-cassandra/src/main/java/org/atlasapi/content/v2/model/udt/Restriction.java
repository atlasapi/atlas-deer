package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "restriction")
public class Restriction {

    private Long id;
    private String canonicalUri;
    private String curie;
    private Set<String> aliasUrls;
    private Set<Alias> aliases;
    private Set<Ref> equivalentTo;
    private Instant lastUpdated;
    private Instant equivalenceUpdate;
    private Boolean restricted;
    private Integer minimumAge;
    private String message;
    private String authority;
    private String rating;

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

    public Boolean getRestricted() {
        return restricted;
    }

    public void setRestricted(Boolean restricted) {
        this.restricted = restricted;
    }

    public Integer getMinimumAge() {
        return minimumAge;
    }

    public void setMinimumAge(Integer minimumAge) {
        this.minimumAge = minimumAge;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getAuthority() {
        return authority;
    }

    public void setAuthority(String authority) {
        this.authority = authority;
    }

    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }

    public Instant getEquivalenceUpdate() {
        return equivalenceUpdate;
    }

    public void setEquivalenceUpdate(Instant equivalenceUpdate) {
        this.equivalenceUpdate = equivalenceUpdate;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
