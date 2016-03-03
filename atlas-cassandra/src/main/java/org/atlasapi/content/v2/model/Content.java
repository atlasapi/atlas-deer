package org.atlasapi.content.v2.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.content.v2.model.udt.BroadcastRef;
import org.atlasapi.content.v2.model.udt.Certificate;
import org.atlasapi.content.v2.model.udt.Clip;
import org.atlasapi.content.v2.model.udt.ContainerRef;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.ContentGroupRef;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.Encoding;
import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.ItemSummary;
import org.atlasapi.content.v2.model.udt.KeyPhrase;
import org.atlasapi.content.v2.model.udt.LocationSummary;
import org.atlasapi.content.v2.model.udt.Priority;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.RelatedLink;
import org.atlasapi.content.v2.model.udt.ReleaseDate;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.model.udt.Synopses;
import org.atlasapi.content.v2.model.udt.Tag;

import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.joda.time.Instant;

@Table(name = "content_v2")
public class Content {
    @PartitionKey
    private Long id;
    private String type;
    private String canonicalUri;
    private String curie;
    private Set<String> aliasUrls;
    @FrozenValue
    private Set<Alias> aliases;
    @FrozenValue
    private Set<Ref> equivalentTo;
    private Instant lastUpdated;
    private Instant equivalenceUpdate;
    private String title;
    private String shortDescription;
    private String mediumDescription;
    private String longDescription;
    @Frozen
    private Synopses synopses;
    private String description;
    private String mediaType;
    private String specialization;
    private Set<String> genres;
    private String publisher;
    private String image;
    @FrozenValue
    private Set<Image> images;
    private String thumbnail;
    private Instant firstSeen;
    private Instant lastFetched;
    private Instant thisOrChildLastUpdated;
    private Boolean scheduleOnly;
    private Boolean activelyPublished;
    private String presentationChannel;
    @Frozen
    private Priority priority;
    @FrozenValue
    private Set<RelatedLink> relatedLinks;
    @FrozenValue
    private List<Clip> clips;
    @FrozenValue
    private Set<KeyPhrase> keyPhrases;
    @FrozenValue
    private List<Tag> tags;
    @FrozenValue
    private List<ContentGroupRef> contentGroupRefs;
    @FrozenValue
    private List<CrewMember> people;
    private Set<String> languages;
    @FrozenValue
    private Set<Certificate> certificates;
    private Integer year;
    @FrozenValue
    private Set<Encoding> manifestedAs;
    private Boolean genericDescription;
    @FrozenValue
    private Set<Ref> eventRefs;
    private String isrc;
    private Long duration;

    private Integer seriesNumber;
    private Integer totalEpisodes;
    @Frozen
    private Ref brandRef;

    @Frozen
    private ContainerRef containerRef;
    private Boolean isLongForm;
    private Boolean blackAndWhite;
    private Set<String> countriesOfOrigin;
    private String sortKey;
    @Frozen
    private ContainerSummary containerSummary;
    @FrozenValue
    private Set<Broadcast> broadcasts;
    @FrozenValue
    private List<SegmentEvent> segmentEvents;
    @FrozenValue
    private Set<Restriction> restrictions;

    private String websiteUrl;
    private Set<String> subtitles;
    @FrozenValue
    private Set<ReleaseDate> releaseDates;

    private Integer episodeNumber;
    private Integer partNumber;
    private Boolean special;

    @FrozenValue
    private List<SeriesRef> seriesRefs;

    @FrozenValue
    private List<ItemRef> itemRefs;
    @Frozen("map<frozen<ItemRef>, frozen<list<frozen<BroadcastRef>>>>")
    private Map<ItemRef, List<BroadcastRef>> upcomingContent;
    @Frozen("map<frozen<ItemRef>, frozen<list<frozen<LocationSummary>>>>")
    private Map<ItemRef, List<LocationSummary>> availableContent;
    @FrozenValue
    private List<ItemSummary> itemSummaries;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public List<Clip> getClips() {
        return clips;
    }

    public void setClips(List<Clip> clips) {
        this.clips = clips;
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

    public String getIsrc() {
        return isrc;
    }

    public void setIsrc(String isrc) {
        this.isrc = isrc;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    public void setSeriesNumber(Integer seriesNumber) {
        this.seriesNumber = seriesNumber;
    }

    public Integer getTotalEpisodes() {
        return totalEpisodes;
    }

    public void setTotalEpisodes(Integer totalEpisodes) {
        this.totalEpisodes = totalEpisodes;
    }

    public Ref getBrandRef() {
        return brandRef;
    }

    public void setBrandRef(Ref brandRef) {
        this.brandRef = brandRef;
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

    public String getWebsiteUrl() {
        return websiteUrl;
    }

    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }

    public Set<String> getSubtitles() {
        return subtitles;
    }

    public void setSubtitles(Set<String> subtitles) {
        this.subtitles = subtitles;
    }

    public Set<ReleaseDate> getReleaseDates() {
        return releaseDates;
    }

    public void setReleaseDates(
            Set<ReleaseDate> releaseDates) {
        this.releaseDates = releaseDates;
    }

    public Integer getEpisodeNumber() {
        return episodeNumber;
    }

    public void setEpisodeNumber(Integer episodeNumber) {
        this.episodeNumber = episodeNumber;
    }

    public Integer getPartNumber() {
        return partNumber;
    }

    public void setPartNumber(Integer partNumber) {
        this.partNumber = partNumber;
    }

    public Boolean getSpecial() {
        return special;
    }

    public void setSpecial(Boolean special) {
        this.special = special;
    }

    public List<SeriesRef> getSeriesRefs() {
        return seriesRefs;
    }

    public void setSeriesRefs(List<SeriesRef> seriesRefs) {
        this.seriesRefs = seriesRefs;
    }

    public List<ItemRef> getItemRefs() {
        return itemRefs;
    }

    public void setItemRefs(List<ItemRef> itemRefs) {
        this.itemRefs = itemRefs;
    }

    public Map<ItemRef, List<BroadcastRef>> getUpcomingContent() {
        return upcomingContent;
    }

    public void setUpcomingContent(
            Map<ItemRef, List<BroadcastRef>> upcomingContent) {
        this.upcomingContent = upcomingContent;
    }

    public Map<ItemRef, List<LocationSummary>> getAvailableContent() {
        return availableContent;
    }

    public void setAvailableContent(
            Map<ItemRef, List<LocationSummary>> availableContent) {
        this.availableContent = availableContent;
    }

    public List<ItemSummary> getItemSummaries() {
        return itemSummaries;
    }

    public void setItemSummaries(
            List<ItemSummary> itemSummaries) {
        this.itemSummaries = itemSummaries;
    }
}
