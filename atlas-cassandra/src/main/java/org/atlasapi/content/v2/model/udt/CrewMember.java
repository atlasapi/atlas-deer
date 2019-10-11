package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.comparison.ExcludeFromObjectComparison;
import org.atlasapi.content.v2.model.Identified;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@UDT(name = "crewmember")
public class CrewMember implements Identified {

    @Field(name = "id") private Long id;
    @Field(name = "canonical_uri") private String canonicalUri;
    @Field(name = "curie") private String curie;
    @Field(name = "alias_urls") private Set<String> aliasUrls;
    @Field(name = "aliases") private Set<Alias> aliases;
    @Field(name = "equiv_to") private Set<Ref> equivalentTo;
    @ExcludeFromObjectComparison
    @Field(name = "last_updated") private Instant lastUpdated;
    @ExcludeFromObjectComparison
    @Field(name = "equiv_update") private Instant equivalenceUpdate;
    @Field(name = "role") private String role;
    @Field(name = "name") private String name;
    @Field(name = "publisher") private String publisher;
    @Field(name = "type") private String type;
    @Field(name = "character") private String character;
    @Field(name = "custom_fields") private Map<String, String> customFields;

    public CrewMember() {}

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

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCharacter() {
        return character;
    }

    public void setCharacter(String character) {
        this.character = character;
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
        CrewMember that = (CrewMember) object;
        return Objects.equals(id, that.id) &&
                Objects.equals(canonicalUri, that.canonicalUri) &&
                Objects.equals(curie, that.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, that.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, that.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, that.equivalentTo) &&
                Objects.equals(role, that.role) &&
                Objects.equals(name, that.name) &&
                Objects.equals(publisher, that.publisher) &&
                Objects.equals(type, that.type) &&
                Objects.equals(character, that.character) &&
                NullOrEmptyEquality.equals(customFields, that.customFields);
    }

    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(id, canonicalUri, curie, aliasUrls, aliases, equivalentTo, role, name, publisher, type, character, customFields);
    }
}
