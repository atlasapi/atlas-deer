package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.content.v2.model.IdentifiedWithoutUpdateTimes;
import org.atlasapi.util.NullOrEmptyEquality;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@UDT(name = "restriction")
public class Restriction implements IdentifiedWithoutUpdateTimes {

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
    @Field(name = "custom_fields") private Map<String, String> customFields;

    public Restriction() {}

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
        Restriction that = (Restriction) object;
        return Objects.equals(id, that.id) &&
                Objects.equals(canonicalUri, that.canonicalUri) &&
                Objects.equals(curie, that.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, that.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, that.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, that.equivalentTo) &&
                Objects.equals(restricted, that.restricted) &&
                Objects.equals(minimumAge, that.minimumAge) &&
                Objects.equals(message, that.message) &&
                Objects.equals(authority, that.authority) &&
                Objects.equals(rating, that.rating) &&
                NullOrEmptyEquality.equals(customFields, that.customFields);
    }

    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(id, canonicalUri, curie, aliasUrls, aliases, equivalentTo, restricted, minimumAge, message, authority, rating, customFields);
    }
}
