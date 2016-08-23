package org.atlasapi.content.v2.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Award;
import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.content.v2.model.udt.BroadcastRef;
import org.atlasapi.content.v2.model.udt.Certificate;
import org.atlasapi.content.v2.model.udt.ContainerRef;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.ContentGroupRef;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.content.v2.model.udt.ItemRef;
import org.atlasapi.content.v2.model.udt.ItemSummary;
import org.atlasapi.content.v2.model.udt.KeyPhrase;
import org.atlasapi.content.v2.model.udt.LocationSummary;
import org.atlasapi.content.v2.model.udt.Priority;
import org.atlasapi.content.v2.model.udt.Rating;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.RelatedLink;
import org.atlasapi.content.v2.model.udt.ReleaseDate;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.Review;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.model.udt.Synopses;
import org.atlasapi.content.v2.model.udt.Tag;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.joda.time.Instant;

@Table(name = "content_v2")
public class Content implements ContentIface {

    @PartitionKey
    @Column(name = "id")
    private Long id;

    @Column(name = "type")
    private String type;

    @Column(name = "canonical_uri")
    private String canonicalUri;

    @Column(name = "curie")
    private String curie;

    @Column(name = "alias_urls")
    private Set<String> aliasUrls;

    @FrozenValue
    @Column(name = "aliases")
    private Set<Alias> aliases;

    @FrozenValue
    @Column(name = "equiv_to")
    private Set<Ref> equivalentTo;

    @Column(name = "last_updated")
    private Instant lastUpdated;

    @Column(name = "equiv_update")
    private Instant equivalenceUpdate;

    @Column(name = "title")
    private String title;

    @Column(name = "short_descr")
    private String shortDescription;

    @Column(name = "medium_descr")
    private String mediumDescription;

    @Column(name = "long_descr")
    private String longDescription;

    @Frozen
    @Column(name = "synopses")
    private Synopses synopses;

    @Column(name = "descr")
    private String description;

    @Column(name = "media_type")
    private String mediaType;

    @Column(name = "specialization")
    private String specialization;

    @Column(name = "genres")
    private Set<String> genres;

    @Column(name = "publisher")
    private String publisher;

    @Column(name = "image")
    private String image;

    @FrozenValue
    @Column(name = "images")
    private Set<Image> images;

    @Column(name = "thumbnail")
    private String thumbnail;

    @Column(name = "first_seen")
    private Instant firstSeen;

    @Column(name = "last_fetched")
    private Instant lastFetched;

    @Column(name = "this_or_child_last_updated")
    private Instant thisOrChildLastUpdated;

    @Column(name = "schedule_only")
    private Boolean scheduleOnly;

    @Column(name = "actively_published")
    private Boolean activelyPublished;

    @Column(name = "presentation_channel")
    private String presentationChannel;

    @Frozen
    @Column(name = "priority")
    private Priority priority;

    @FrozenValue
    @Column(name = "related_links")
    private Set<RelatedLink> relatedLinks;

    @FrozenValue
    @Column(name = "awards")
    private Set<Award> awards;

    @FrozenValue
    @Column(name = "key_phrases")
    private Set<KeyPhrase> keyPhrases;

    @FrozenValue
    @Column(name = "tags")
    private List<Tag> tags;

    @FrozenValue
    @Column(name = "content_group_refs")
    private Set<ContentGroupRef> contentGroupRefs;

    @FrozenValue
    @Column(name = "people")
    private List<CrewMember> people;

    @Column(name = "languages")
    private Set<String> languages;

    @FrozenValue
    @Column(name = "certificates")
    private Set<Certificate> certificates;

    @Column(name = "year")
    private Integer year;

    @Column(name = "generic_description")
    private Boolean genericDescription;

    @FrozenValue
    @Column(name = "event_refs")
    private Set<Ref> eventRefs;

    @Column(name = "isrc")
    private String isrc;

    @Column(name = "duration")
    private Long duration;

    @Column(name = "series_nr")
    private Integer seriesNumber;

    @Column(name = "total_episodes")
    private Integer totalEpisodes;

    @Frozen
    @Column(name = "brand_ref")
    private Ref brandRef;

    @Frozen
    @Column(name = "container_ref")
    private ContainerRef containerRef;

    @Column(name = "is_long_form")
    private Boolean isLongForm;

    @Column(name = "black_and_white")
    private Boolean blackAndWhite;

    @Column(name = "countries_of_origin")
    private Set<String> countriesOfOrigin;

    @Column(name = "sort_key")
    private String sortKey;

    @Frozen
    @Column(name = "container_summary")
    private ContainerSummary containerSummary;

    @FrozenValue
    @Column(name = "broadcasts")
    private Set<Broadcast> broadcasts;

    @FrozenValue
    @Column(name = "segment_events")
    private List<SegmentEvent> segmentEvents;

    @FrozenValue
    @Column(name = "restrictions")
    private Set<Restriction> restrictions;

    @Column(name = "website_url")
    private String websiteUrl;

    @Column(name = "subtitles")
    private Set<String> subtitles;

    @FrozenValue
    @Column(name = "release_dates")
    private Set<ReleaseDate> releaseDates;

    @Column(name = "episode_nr")
    private Integer episodeNumber;

    @Column(name = "part_nr")
    private Integer partNumber;

    @Column(name = "special")
    private Boolean special;

    @FrozenValue
    @Column(name = "series_refs")
    private Set<SeriesRef> seriesRefs;

    @FrozenValue
    @Column(name = "item_refs")
    private Set<ItemRef> itemRefs;

    @Frozen("map<frozen<ItemRef>, frozen<list<BroadcastRef>>>")
    @Column(name = "upcoming")
    private Map<ItemRef, List<BroadcastRef>> upcomingContent;

    @Frozen("map<frozen<ItemRef>, frozen<list<LocationSummary>>>")
    @Column(name = "available")
    private Map<ItemRef, List<LocationSummary>> availableContent;

    @FrozenValue
    @Column(name = "item_summaries")
    private Set<ItemSummary> itemSummaries;

    @FrozenValue
    @Column(name = "reviews")
    private Set<Review> reviews;

    @FrozenValue
    @Column(name = "ratings")
    private Set<Rating> ratings;

    @Column(name = "clips")
    private Clip.Wrapper clips;

    @Column(name = "encodings")
    private Encoding.Wrapper encodings;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String getCanonicalUri() {
        return canonicalUri;
    }

    @Override
    public void setCanonicalUri(String canonicalUri) {
        this.canonicalUri = canonicalUri;
    }

    @Override
    public String getCurie() {
        return curie;
    }

    @Override
    public void setCurie(String curie) {
        this.curie = curie;
    }

    @Override
    public Set<String> getAliasUrls() {
        return aliasUrls;
    }

    @Override
    public void setAliasUrls(Set<String> aliasUrls) {
        this.aliasUrls = aliasUrls;
    }

    @Override
    public Set<Alias> getAliases() {
        return aliases;
    }

    @Override
    public void setAliases(Set<Alias> aliases) {
        this.aliases = aliases;
    }

    @Override
    public Set<Ref> getEquivalentTo() {
        return equivalentTo;
    }

    @Override
    public void setEquivalentTo(Set<Ref> equivalentTo) {
        this.equivalentTo = equivalentTo;
    }

    @Override
    public Instant getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Override
    public Instant getEquivalenceUpdate() {
        return equivalenceUpdate;
    }

    @Override
    public void setEquivalenceUpdate(Instant equivalenceUpdate) {
        this.equivalenceUpdate = equivalenceUpdate;
    }

    @Override
    public String getTitle() {
        return title;
    }

    @Override
    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String getShortDescription() {
        return shortDescription;
    }

    @Override
    public void setShortDescription(String shortDescription) {
        this.shortDescription = shortDescription;
    }

    @Override
    public String getMediumDescription() {
        return mediumDescription;
    }

    @Override
    public void setMediumDescription(String mediumDescription) {
        this.mediumDescription = mediumDescription;
    }

    @Override
    public String getLongDescription() {
        return longDescription;
    }

    @Override
    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

    @Override
    public Synopses getSynopses() {
        return synopses;
    }

    @Override
    public void setSynopses(Synopses synopses) {
        this.synopses = synopses;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getMediaType() {
        return mediaType;
    }

    @Override
    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    @Override
    public String getSpecialization() {
        return specialization;
    }

    @Override
    public void setSpecialization(String specialization) {
        this.specialization = specialization;
    }

    @Override
    public Set<String> getGenres() {
        return genres;
    }

    @Override
    public void setGenres(Set<String> genres) {
        this.genres = genres;
    }

    @Override
    public String getPublisher() {
        return publisher;
    }

    @Override
    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    @Override
    public String getImage() {
        return image;
    }

    @Override
    public void setImage(String image) {
        this.image = image;
    }

    @Override
    public Set<Image> getImages() {
        return images;
    }

    @Override
    public void setImages(Set<Image> images) {
        this.images = images;
    }

    @Override
    public String getThumbnail() {
        return thumbnail;
    }

    @Override
    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    @Override
    public Instant getFirstSeen() {
        return firstSeen;
    }

    @Override
    public void setFirstSeen(Instant firstSeen) {
        this.firstSeen = firstSeen;
    }

    @Override
    public Instant getLastFetched() {
        return lastFetched;
    }

    @Override
    public void setLastFetched(Instant lastFetched) {
        this.lastFetched = lastFetched;
    }

    @Override
    public Instant getThisOrChildLastUpdated() {
        return thisOrChildLastUpdated;
    }

    @Override
    public void setThisOrChildLastUpdated(Instant thisOrChildLastUpdated) {
        this.thisOrChildLastUpdated = thisOrChildLastUpdated;
    }

    @Override
    public Boolean getScheduleOnly() {
        return scheduleOnly;
    }

    @Override
    public void setScheduleOnly(Boolean scheduleOnly) {
        this.scheduleOnly = scheduleOnly;
    }

    @Override
    public Boolean getActivelyPublished() {
        return activelyPublished;
    }

    @Override
    public void setActivelyPublished(Boolean activelyPublished) {
        this.activelyPublished = activelyPublished;
    }

    @Override
    public String getPresentationChannel() {
        return presentationChannel;
    }

    @Override
    public void setPresentationChannel(String presentationChannel) {
        this.presentationChannel = presentationChannel;
    }

    @Override
    public Priority getPriority() {
        return priority;
    }

    @Override
    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    @Override
    public Set<RelatedLink> getRelatedLinks() {
        return relatedLinks;
    }

    @Override
    public void setRelatedLinks(Set<RelatedLink> relatedLinks) {
        this.relatedLinks = relatedLinks;
    }

    @Override
    public Set<Award> getAwards() {
        return awards;
    }

    @Override
    public void setAwards(Set<Award> awards) {
        this.awards = awards;
    }

    @Override
    public Set<Review> getReviews() {
        return this.reviews;
    }

    @Override
    public void setReviews(Set<Review> reviews) {
        this.reviews = reviews;
    }

    @Override
    public Set<Rating> getRatings() {
        return this.ratings;
    }

    @Override
    public void setRatings(Set<Rating> ratings) {
        this.ratings = ratings;
    }

    @Override
    public Set<KeyPhrase> getKeyPhrases() {
        return keyPhrases;
    }

    @Override
    public void setKeyPhrases(Set<KeyPhrase> keyPhrases) {
        this.keyPhrases = keyPhrases;
    }

    @Override
    public List<Tag> getTags() {
        return tags;
    }

    @Override
    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    @Override
    public Set<ContentGroupRef> getContentGroupRefs() {
        return contentGroupRefs;
    }

    @Override
    public void setContentGroupRefs(Set<ContentGroupRef> contentGroupRefs) {
        this.contentGroupRefs = contentGroupRefs;
    }

    @Override
    public List<CrewMember> getPeople() {
        return people;
    }

    @Override
    public void setPeople(List<CrewMember> people) {
        this.people = people;
    }

    @Override
    public Set<String> getLanguages() {
        return languages;
    }

    @Override
    public void setLanguages(Set<String> languages) {
        this.languages = languages;
    }

    @Override
    public Set<Certificate> getCertificates() {
        return certificates;
    }

    @Override
    public void setCertificates(
            Set<Certificate> certificates) {
        this.certificates = certificates;
    }

    @Override
    public Integer getYear() {
        return year;
    }

    @Override
    public void setYear(Integer year) {
        this.year = year;
    }

    @Override
    public Boolean getGenericDescription() {
        return genericDescription;
    }

    @Override
    public void setGenericDescription(Boolean genericDescription) {
        this.genericDescription = genericDescription;
    }

    @Override
    public Set<Ref> getEventRefs() {
        return eventRefs;
    }

    @Override
    public void setEventRefs(Set<Ref> eventRefs) {
        this.eventRefs = eventRefs;
    }

    @Override
    public Clip.Wrapper getClips() {
        return clips;
    }

    @Override
    public void setClips(Clip.Wrapper clips) {
        this.clips = clips;
    }

    @Override
    public Encoding.Wrapper getEncodings() {
        return encodings;
    }

    @Override
    public void setEncodings(Encoding.Wrapper encodings) {
        this.encodings = encodings;
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

    public Set<SeriesRef> getSeriesRefs() {
        return seriesRefs;
    }

    public void setSeriesRefs(Set<SeriesRef> seriesRefs) {
        this.seriesRefs = seriesRefs;
    }

    public Set<ItemRef> getItemRefs() {
        return itemRefs;
    }

    public void setItemRefs(Set<ItemRef> itemRefs) {
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

    public Set<ItemSummary> getItemSummaries() {
        return itemSummaries;
    }

    public void setItemSummaries(Set<ItemSummary> itemSummaries) {
        this.itemSummaries = itemSummaries;
    }
}
