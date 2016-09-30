package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import org.atlasapi.content.v2.model.Identified;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "restriction")
public class Restriction implements Identified {

    @Field(name = "id") private Long id;
    @Field(name = "canonical_uri") private String canonicalUri;
    @Field(name = "curie") private String curie;
    @Field(name = "alias_urls") private Set<String> aliasUrls;
    @Field(name = "aliases") private Set<Alias> aliases;
    @Field(name = "equiv_to") private Set<Ref> equivalentTo;

    @Field(name = "restricted") private Boolean restricted;
    @Field(name = "minimum_age") private Integer minimumAge;
    @Field(name = "message") private String message;
    @Field(name = "authority") private String authority;
    @Field(name = "rating") private String rating;

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

    @Override
    public Instant getLastUpdated() {
        throw new UnsupportedOperationException("stored as part of a map value against this key");
    }

    @Override
    public void setLastUpdated(Instant lastUpdated) {
        throw new UnsupportedOperationException("stored as part of a map value against this key");
    }

    @Override
    public Instant getEquivalenceUpdate() {
        throw new UnsupportedOperationException("stored as part of a map value against this key");
    }

    @Override
    public void setEquivalenceUpdate(Instant equivalenceUpdate) {
        throw new UnsupportedOperationException("stored as part of a map value against this key");
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
}
