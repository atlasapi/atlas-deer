package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "crewmember")
public class CrewMember {

    @Field(name = "i")
    private Long id;

    @Field(name = "c")
    private String canonicalUri;

    @Field(name = "cu")
    private String curie;

    @Field(name = "au")
    private Set<String> aliasUrls;

    @Field(name = "a")
    private Set<Alias> aliases;

    @Field(name = "e")
    private Set<Ref> equivalentTo;

    @Field(name = "lu")
    private Instant lastUpdated;

    @Field(name = "eu")
    private Instant equivalenceUpdate;

    @Field(name = "r")
    private String role;

    @Field(name = "n")
    private String name;

    @Field(name = "p")
    private String publisher;

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
}
