package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "segmentevent")
public class SegmentEvent {

    @Field(name = "i") private Long id;
    @Field(name = "c") private String canonicalUri;
    @Field(name = "cu") private String curie;
    @Field(name = "au") private Set<String> aliasUrls;
    @Field(name = "a") private Set<Alias> aliases;
    @Field(name = "e") private Set<Ref> equivalentTo;
    @Field(name = "lu") private Instant lastUpdated;
    @Field(name = "eu") private Instant equivalenceUpdate;

    @Field(name = "p") private Integer position;
    @Field(name = "o") private Long offset;
    @Field(name = "ic") private Boolean isChapter;
    @Field(name = "d") private Description description;
    @Field(name = "sr") private Ref segmentRef;
    @Field(name = "vi") private String versionId;
    @Field(name = "pb") private String publisher;

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

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Boolean getIsChapter() {
        return isChapter;
    }

    public void setIsChapter(Boolean isChapter) {
        this.isChapter = isChapter;
    }

    public Description getDescription() {
        return description;
    }

    public void setDescription(Description description) {
        this.description = description;
    }

    public Ref getSegmentRef() {
        return segmentRef;
    }

    public void setSegmentRef(Ref segmentRef) {
        this.segmentRef = segmentRef;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
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
