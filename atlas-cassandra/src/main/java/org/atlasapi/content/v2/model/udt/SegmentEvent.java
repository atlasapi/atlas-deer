package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.content.v2.model.Identified;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@UDT(name = "segmentevent")
public class SegmentEvent implements Identified {

    public static final Comparator<SegmentEvent> COMPARATOR =
            Comparator.comparing(SegmentEvent::getPosition, Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(SegmentEvent::getCanonicalUri);

    @Field(name = "id") private Long id;
    @Field(name = "canonical_uri") private String canonicalUri;
    @Field(name = "curie") private String curie;
    @Field(name = "alias_urls") private Set<String> aliasUrls;
    @Field(name = "aliases") private Set<Alias> aliases;
    @Field(name = "equiv_to") private Set<Ref> equivalentTo;
    @Field(name = "last_updated") private Instant lastUpdated;
    @Field(name = "equiv_update") private Instant equivalenceUpdate;

    @Field(name = "position") private Integer position;
    @Field(name = "offset") private Long offset;
    @Field(name = "is_chapter") private Boolean isChapter;
    @Field(name = "descr") private Description description;
    @Field(name = "segment_ref") private Ref segmentRef;
    @Field(name = "version_id") private String versionId;
    @Field(name = "publisher") private String publisher;
    @Field(name = "custom_fields") private Map<String, String> customFields;

    public SegmentEvent() {}

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
        SegmentEvent that = (SegmentEvent) object;
        return Objects.equals(id, that.id) &&
                Objects.equals(canonicalUri, that.canonicalUri) &&
                Objects.equals(curie, that.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, that.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, that.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, that.equivalentTo) &&
                Objects.equals(position, that.position) &&
                Objects.equals(offset, that.offset) &&
                Objects.equals(isChapter, that.isChapter) &&
                Objects.equals(description, that.description) &&
                Objects.equals(segmentRef, that.segmentRef) &&
                Objects.equals(versionId, that.versionId) &&
                Objects.equals(publisher, that.publisher) &&
                NullOrEmptyEquality.equals(customFields, that.customFields);
    }

    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(id, canonicalUri, curie, aliasUrls, aliases, equivalentTo, position, offset, isChapter, description, segmentRef, versionId, publisher, customFields);
    }
}
