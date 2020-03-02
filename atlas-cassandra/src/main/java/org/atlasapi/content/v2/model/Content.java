package org.atlasapi.content.v2.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenKey;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.Award;
import org.atlasapi.content.v2.model.udt.Broadcast;
import org.atlasapi.content.v2.model.udt.Certificate;
import org.atlasapi.content.v2.model.udt.ContainerRef;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.ContentGroupRef;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.content.v2.model.udt.Interval;
import org.atlasapi.content.v2.model.udt.ItemRefAndBroadcastRefs;
import org.atlasapi.content.v2.model.udt.ItemRefAndItemSummary;
import org.atlasapi.content.v2.model.udt.ItemRefAndLocationSummaries;
import org.atlasapi.content.v2.model.udt.KeyPhrase;
import org.atlasapi.content.v2.model.udt.PartialItemRef;
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
import org.atlasapi.content.v2.model.udt.UpdateTimes;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

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

    @FrozenKey
    @FrozenValue
    @Column(name = "images")
    private Map<Image, Interval> images;

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

    @FrozenKey
    @FrozenValue
    @Column(name = "broadcasts")
    private Map<String, Broadcast> broadcasts;

    @FrozenValue
    @Column(name = "segment_events")
    private List<SegmentEvent> segmentEvents;

    @FrozenKey
    @FrozenValue
    @Column(name = "restrictions")
    private Map<Restriction, UpdateTimes> restrictions;

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

    @FrozenKey
    @FrozenValue
    @Column(name = "series_refs")
    private Map<Ref, SeriesRef> seriesRefs;

    @FrozenKey
    @FrozenValue
    @Column(name = "item_refs")
    private Map<Ref, PartialItemRef> itemRefs;

    @FrozenKey
    @FrozenValue
    @Column(name = "upcoming")
    private Map<Ref, ItemRefAndBroadcastRefs> upcomingContent;

    @FrozenKey
    @FrozenValue
    @Column(name = "available")
    private Map<Ref, ItemRefAndLocationSummaries> availableContent;

    @FrozenKey
    @FrozenValue
    @Column(name = "item_summaries")
    private Map<Ref, ItemRefAndItemSummary> itemSummaries;

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

    @Column(name = "custom_fields")
    private Map<String, String> customFields;

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
    public Map<Image, Interval> getImages() {
        return images;
    }

    @Override
    public void setImages(Map<Image, Interval> images) {
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
    public void setCertificates(Set<Certificate> certificates) {
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

    @Override
    public Map<String, String> getCustomFields() {
        return customFields;
    }

    @Override
    public void setCustomFields(Map<String, String> customFields) {
        this.customFields = customFields;
    }

    public String getIsrc() {
        return isrc;
    }

    public void setIsrc(String isrc) {
        this.isrc = isrc;
    }

    @Nullable
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

    public Map<String, Broadcast> getBroadcasts() {
        return broadcasts;
    }

    public void setBroadcasts(Map<String, Broadcast> broadcasts) {
        this.broadcasts = broadcasts;
    }

    public List<SegmentEvent> getSegmentEvents() {
        return segmentEvents;
    }

    public void setSegmentEvents(
            List<SegmentEvent> segmentEvents) {
        this.segmentEvents = segmentEvents;
    }

    public Map<Restriction, UpdateTimes> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(Map<Restriction, UpdateTimes> restrictions) {
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

    public void setReleaseDates(Set<ReleaseDate> releaseDates) {
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

    public Map<Ref, SeriesRef> getSeriesRefs() {
        return seriesRefs;
    }

    public void setSeriesRefs(Map<Ref, SeriesRef> seriesRefs) {
        this.seriesRefs = seriesRefs;
    }

    public Map<Ref, PartialItemRef> getItemRefs() {
        return itemRefs;
    }

    public void setItemRefs(Map<Ref, PartialItemRef> itemRefs) {
        this.itemRefs = itemRefs;
    }

    public Map<Ref, ItemRefAndBroadcastRefs> getUpcomingContent() {
        return upcomingContent;
    }

    public void setUpcomingContent(Map<Ref, ItemRefAndBroadcastRefs> upcomingContent) {
        this.upcomingContent = upcomingContent;
    }

    public Map<Ref, ItemRefAndLocationSummaries> getAvailableContent() {
        return availableContent;
    }

    public void setAvailableContent(Map<Ref, ItemRefAndLocationSummaries> availableContent) {
        this.availableContent = availableContent;
    }

    public Map<Ref, ItemRefAndItemSummary> getItemSummaries() {
        return itemSummaries;
    }

    public void setItemSummaries(Map<Ref, ItemRefAndItemSummary> itemSummaries) {
        this.itemSummaries = itemSummaries;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Content content = (Content) object;
        return Objects.equals(id, content.id) &&
                Objects.equals(type, content.type) &&
                Objects.equals(canonicalUri, content.canonicalUri) &&
                Objects.equals(curie, content.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, content.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, content.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, content.equivalentTo) &&
                Objects.equals(title, content.title) &&
                Objects.equals(shortDescription, content.shortDescription) &&
                Objects.equals(mediumDescription, content.mediumDescription) &&
                Objects.equals(longDescription, content.longDescription) &&
                Objects.equals(synopses, content.synopses) &&
                Objects.equals(description, content.description) &&
                Objects.equals(mediaType, content.mediaType) &&
                Objects.equals(specialization, content.specialization) &&
                NullOrEmptyEquality.equals(genres, content.genres) &&
                Objects.equals(publisher, content.publisher) &&
                Objects.equals(image, content.image) &&
                NullOrEmptyEquality.equals(images, content.images) &&
                Objects.equals(thumbnail, content.thumbnail) &&
                Objects.equals(scheduleOnly, content.scheduleOnly) &&
                Objects.equals(activelyPublished, content.activelyPublished) &&
                Objects.equals(presentationChannel, content.presentationChannel) &&
                Objects.equals(priority, content.priority) &&
                NullOrEmptyEquality.equals(relatedLinks, content.relatedLinks) &&
                NullOrEmptyEquality.equals(awards, content.awards) &&
                NullOrEmptyEquality.equals(keyPhrases, content.keyPhrases) &&
                NullOrEmptyEquality.equals(tags, content.tags) &&
                NullOrEmptyEquality.equals(contentGroupRefs, content.contentGroupRefs) &&
                NullOrEmptyEquality.equals(people, content.people) &&
                NullOrEmptyEquality.equals(languages, content.languages) &&
                NullOrEmptyEquality.equals(certificates, content.certificates) &&
                Objects.equals(year, content.year) &&
                Objects.equals(genericDescription, content.genericDescription) &&
                NullOrEmptyEquality.equals(eventRefs, content.eventRefs) &&
                Objects.equals(isrc, content.isrc) &&
                Objects.equals(duration, content.duration) &&
                Objects.equals(seriesNumber, content.seriesNumber) &&
                Objects.equals(totalEpisodes, content.totalEpisodes) &&
                Objects.equals(brandRef, content.brandRef) &&
                Objects.equals(containerRef, content.containerRef) &&
                Objects.equals(isLongForm, content.isLongForm) &&
                Objects.equals(blackAndWhite, content.blackAndWhite) &&
                NullOrEmptyEquality.equals(countriesOfOrigin, content.countriesOfOrigin) &&
                Objects.equals(sortKey, content.sortKey) &&
                Objects.equals(containerSummary, content.containerSummary) &&
                NullOrEmptyEquality.equals(broadcasts, content.broadcasts) &&
                NullOrEmptyEquality.equals(segmentEvents, content.segmentEvents) &&
                NullOrEmptyEquality.equals(restrictions, content.restrictions) &&
                Objects.equals(websiteUrl, content.websiteUrl) &&
                NullOrEmptyEquality.equals(subtitles, content.subtitles) &&
                NullOrEmptyEquality.equals(releaseDates, content.releaseDates) &&
                Objects.equals(episodeNumber, content.episodeNumber) &&
                Objects.equals(partNumber, content.partNumber) &&
                Objects.equals(special, content.special) &&
                NullOrEmptyEquality.equals(seriesRefs, content.seriesRefs) &&
                NullOrEmptyEquality.equals(itemRefs, content.itemRefs) &&
                NullOrEmptyEquality.equals(upcomingContent, content.upcomingContent) &&
                NullOrEmptyEquality.equals(availableContent, content.availableContent) &&
                NullOrEmptyEquality.equals(itemSummaries, content.itemSummaries) &&
                NullOrEmptyEquality.equals(reviews, content.reviews) &&
                NullOrEmptyEquality.equals(ratings, content.ratings) &&
                Objects.equals(clips, content.clips) &&
                Objects.equals(encodings, content.encodings) &&
                NullOrEmptyEquality.equals(customFields, content.customFields);
    }


    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(id, type, canonicalUri, curie, aliasUrls, aliases, equivalentTo, title, shortDescription, mediumDescription, longDescription, synopses, description, mediaType, specialization, genres, publisher, image, images, thumbnail, scheduleOnly, activelyPublished, presentationChannel, priority, relatedLinks, awards, keyPhrases, tags, contentGroupRefs, people, languages, certificates, year, genericDescription, eventRefs, isrc, duration, seriesNumber, totalEpisodes, brandRef, containerRef, isLongForm, blackAndWhite, countriesOfOrigin, sortKey, containerSummary, broadcasts, segmentEvents, restrictions, websiteUrl, subtitles, releaseDates, episodeNumber, partNumber, special, seriesRefs, itemRefs, upcomingContent, availableContent, itemSummaries, reviews, ratings, clips, encodings, customFields);
    }
}
