package org.atlasapi.content.v2.model.udt;

import java.util.List;
import java.util.Set;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "clip")
public class Clip {

    @Field(name = "i") private Long id;
    @Field(name = "c") private String canonicalUri;
    @Field(name = "cu") private String curie;
    @Field(name = "au") private Set<String> aliasUrls;
    @Field(name = "a") private Set<Alias> aliases;
    @Field(name = "e") private Set<Ref> equivalentTo;
    @Field(name = "lu") private Instant lastUpdated;
    @Field(name = "eu") private Instant equivalenceUpdate;
    @Field(name = "tt") private String title;
    @Field(name = "sd") private String shortDescription;
    @Field(name = "md") private String mediumDescription;
    @Field(name = "ld") private String longDescription;
    @Field(name = "sn") private Synopses synopses;
    @Field(name = "ds") private String description;
    @Field(name = "mt") private String mediaType;
    @Field(name = "sp") private String specialization;
    @Field(name = "g") private Set<String> genres;
    @Field(name = "pb") private String publisher;
    @Field(name = "im") private String image;
    @Field(name = "ims") private Set<Image> images;
    @Field(name = "th") private String thumbnail;
    @Field(name = "fs") private Instant firstSeen;
    @Field(name = "lf") private Instant lastFetched;
    @Field(name = "toclu") private Instant thisOrChildLastUpdated;
    @Field(name = "so") private Boolean scheduleOnly;
    @Field(name = "ap") private Boolean activelyPublished;
    @Field(name = "pc") private String presentationChannel;
    @Field(name = "pr") private Priority priority;
    @Field(name = "rl") private Set<RelatedLink> relatedLinks;
    @Field(name = "kp") private Set<KeyPhrase> keyPhrases;
    @Field(name = "t") private List<Tag> tags;
    @Field(name = "cgr") private List<ContentGroupRef> contentGroupRefs;
    @Field(name = "pp") private List<CrewMember> people;
    @Field(name = "lng") private Set<String> languages;
    @Field(name = "cr") private Set<Certificate> certificates;
    @Field(name = "yr") private Integer year;
    @Field(name = "ma") private Set<Encoding> manifestedAs;
    @Field(name = "gd") private Boolean genericDescription;
    @Field(name = "er") private Set<Ref> eventRefs;
    @Field(name = "cnr") private ContainerRef containerRef;
    @Field(name = "ilf") private Boolean isLongForm;
    @Field(name = "bnw") private Boolean blackAndWhite;
    @Field(name = "coo") private Set<String> countriesOfOrigin;
    @Field(name = "sk") private String sortKey;
    @Field(name = "cns") private ContainerSummary containerSummary;
    @Field(name = "bc") private Set<Broadcast> broadcasts;
    @Field(name = "sev") private List<SegmentEvent> segmentEvents;
    @Field(name = "rr") private Set<Restriction> restrictions;
    @Field(name = "co") private String clipOf;

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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getShortDescription() {
        return shortDescription;
    }

    public void setShortDescription(String shortDescription) {
        this.shortDescription = shortDescription;
    }

    public String getMediumDescription() {
        return mediumDescription;
    }

    public void setMediumDescription(String mediumDescription) {
        this.mediumDescription = mediumDescription;
    }

    public String getLongDescription() {
        return longDescription;
    }

    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

    public Synopses getSynopses() {
        return synopses;
    }

    public void setSynopses(Synopses synopses) {
        this.synopses = synopses;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    public String getSpecialization() {
        return specialization;
    }

    public void setSpecialization(String specialization) {
        this.specialization = specialization;
    }

    public Set<String> getGenres() {
        return genres;
    }

    public void setGenres(Set<String> genres) {
        this.genres = genres;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public Set<Image> getImages() {
        return images;
    }

    public void setImages(Set<Image> images) {
        this.images = images;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    public Instant getFirstSeen() {
        return firstSeen;
    }

    public void setFirstSeen(Instant firstSeen) {
        this.firstSeen = firstSeen;
    }

    public Instant getLastFetched() {
        return lastFetched;
    }

    public void setLastFetched(Instant lastFetched) {
        this.lastFetched = lastFetched;
    }

    public Instant getThisOrChildLastUpdated() {
        return thisOrChildLastUpdated;
    }

    public void setThisOrChildLastUpdated(Instant thisOrChildLastUpdated) {
        this.thisOrChildLastUpdated = thisOrChildLastUpdated;
    }

    public Boolean getScheduleOnly() {
        return scheduleOnly;
    }

    public void setScheduleOnly(Boolean scheduleOnly) {
        this.scheduleOnly = scheduleOnly;
    }

    public Boolean getActivelyPublished() {
        return activelyPublished;
    }

    public void setActivelyPublished(Boolean activelyPublished) {
        this.activelyPublished = activelyPublished;
    }

    public String getPresentationChannel() {
        return presentationChannel;
    }

    public void setPresentationChannel(String presentationChannel) {
        this.presentationChannel = presentationChannel;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public Set<RelatedLink> getRelatedLinks() {
        return relatedLinks;
    }

    public void setRelatedLinks(
            Set<RelatedLink> relatedLinks) {
        this.relatedLinks = relatedLinks;
    }

    public Set<KeyPhrase> getKeyPhrases() {
        return keyPhrases;
    }

    public void setKeyPhrases(Set<KeyPhrase> keyPhrases) {
        this.keyPhrases = keyPhrases;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    public List<ContentGroupRef> getContentGroupRefs() {
        return contentGroupRefs;
    }

    public void setContentGroupRefs(
            List<ContentGroupRef> contentGroupRefs) {
        this.contentGroupRefs = contentGroupRefs;
    }

    public List<CrewMember> getPeople() {
        return people;
    }

    public void setPeople(List<CrewMember> people) {
        this.people = people;
    }

    public Set<String> getLanguages() {
        return languages;
    }

    public void setLanguages(Set<String> languages) {
        this.languages = languages;
    }

    public Set<Certificate> getCertificates() {
        return certificates;
    }

    public void setCertificates(
            Set<Certificate> certificates) {
        this.certificates = certificates;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Set<Encoding> getManifestedAs() {
        return manifestedAs;
    }

    public void setManifestedAs(
            Set<Encoding> manifestedAs) {
        this.manifestedAs = manifestedAs;
    }

    public Boolean getGenericDescription() {
        return genericDescription;
    }

    public void setGenericDescription(Boolean genericDescription) {
        this.genericDescription = genericDescription;
    }

    public Set<Ref> getEventRefs() {
        return eventRefs;
    }

    public void setEventRefs(Set<Ref> eventRefs) {
        this.eventRefs = eventRefs;
    }

    public ContainerRef getContainerRef() {
        return containerRef;
    }

    public void setContainerRef(ContainerRef containerRef) {
        this.containerRef = containerRef;
    }

    public Boolean getIsLongForm() {
        return isLongForm;
    }

    public void setIsLongForm(Boolean longForm) {
        isLongForm = longForm;
    }

    public Boolean getBlackAndWhite() {
        return blackAndWhite;
    }

    public void setBlackAndWhite(Boolean blackAndWhite) {
        this.blackAndWhite = blackAndWhite;
    }

    public Set<String> getCountriesOfOrigin() {
        return countriesOfOrigin;
    }

    public void setCountriesOfOrigin(Set<String> countriesOfOrigin) {
        this.countriesOfOrigin = countriesOfOrigin;
    }

    public String getSortKey() {
        return sortKey;
    }

    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }

    public ContainerSummary getContainerSummary() {
        return containerSummary;
    }

    public void setContainerSummary(
            ContainerSummary containerSummary) {
        this.containerSummary = containerSummary;
    }

    public Set<Broadcast> getBroadcasts() {
        return broadcasts;
    }

    public void setBroadcasts(Set<Broadcast> broadcasts) {
        this.broadcasts = broadcasts;
    }

    public List<SegmentEvent> getSegmentEvents() {
        return segmentEvents;
    }

    public void setSegmentEvents(
            List<SegmentEvent> segmentEvents) {
        this.segmentEvents = segmentEvents;
    }

    public Set<Restriction> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(
            Set<Restriction> restrictions) {
        this.restrictions = restrictions;
    }

    public String getClipOf() {
        return clipOf;
    }

    public void setClipOf(String clipOf) {
        this.clipOf = clipOf;
    }
}
