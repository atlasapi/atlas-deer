package org.atlasapi.content.v2.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.comparison.ExcludeFromObjectComparison;
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
import org.atlasapi.content.v2.model.udt.KeyPhrase;
import org.atlasapi.content.v2.model.udt.Priority;
import org.atlasapi.content.v2.model.udt.Rating;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.RelatedLink;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.Review;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.Synopses;
import org.atlasapi.content.v2.model.udt.Tag;
import org.atlasapi.content.v2.model.udt.UpdateTimes;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Clip implements ContentIface {

    private Long id;
    private String canonicalUri;
    private String curie;
    private Set<String> aliasUrls;
    private Set<Alias> aliases;
    private Set<Ref> equivalentTo;
    @ExcludeFromObjectComparison
    private Instant lastUpdated;
    @ExcludeFromObjectComparison
    private Instant equivalenceUpdate;
    private String title;
    private String shortDescription;
    private String mediumDescription;
    private String longDescription;
    private Synopses synopses;
    private String description;
    private String mediaType;
    private String specialization;
    private Set<String> genres;
    private String publisher;
    private String image;
    private Map<Image, Interval> images;
    private String thumbnail;
    @ExcludeFromObjectComparison
    private Instant firstSeen;
    @ExcludeFromObjectComparison
    private Instant lastFetched;
    @ExcludeFromObjectComparison
    private Instant thisOrChildLastUpdated;
    private Boolean scheduleOnly;
    private Boolean activelyPublished;
    private String presentationChannel;
    private Priority priority;
    private Set<RelatedLink> relatedLinks;
    private Set<Award> awards;
    private Set<KeyPhrase> keyPhrases;
    private List<Tag> tags;
    private Set<ContentGroupRef> contentGroupRefs;
    private List<CrewMember> people;
    private Set<String> languages;
    private Set<Certificate> certificates;
    private Integer year;
    private Set<Encoding> manifestedAs;
    private Boolean genericDescription;
    private Set<Ref> eventRefs;
    private ContainerRef containerRef;
    private Boolean isLongForm;
    private Boolean blackAndWhite;
    private Set<String> countriesOfOrigin;
    private String sortKey;
    private ContainerSummary containerSummary;
    private Map<String, Broadcast> broadcasts;
    private List<SegmentEvent> segmentEvents;
    private List<RestrictionWithTimes> restrictions;
    private String clipOf;
    private Set<Review> reviews;
    private Set<Rating> ratings;
    private Encoding.Wrapper encodings;
    private Map<String, String> customFields;

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

    @JsonIgnore
    public Map<Image, Interval> getImages() {
        return images;
    }

    @JsonIgnore
    public void setImages(Map<Image, Interval> images) {
        this.images = images;
    }

    @JsonProperty("images")
    public List<ImageWithInterval> getImagesJson() {
        return images.entrySet()
                .stream()
                .map(entry -> new ImageWithInterval(entry.getKey(), entry.getValue()))
                .collect(MoreCollectors.toImmutableList());
    }

    public void setImagesJson(@JsonProperty("images") List<ImageWithInterval> imagesJson) {
        this.images = imagesJson.stream().collect(MoreCollectors.toImmutableMap(
                ImageWithInterval::getImage,
                ImageWithInterval::getInterval
        ));
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

    public void setRelatedLinks(Set<RelatedLink> relatedLinks) {
        this.relatedLinks = relatedLinks;
    }

    public Set<Award> getAwards() {
        return awards;
    }

    public void setAwards(Set<Award> awards) {
        this.awards = awards;
    }

    public Set<Review> getReviews() {
        return this.reviews;
    }

    public void setReviews(Set<Review> reviews) {
        this.reviews = reviews;
    }

    public Set<Rating> getRatings() {
        return this.ratings;
    }

    public void setRatings(Set<Rating> ratings) {
        this.ratings = ratings;
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

    public Set<ContentGroupRef> getContentGroupRefs() {
        return contentGroupRefs;
    }

    public void setContentGroupRefs(Set<ContentGroupRef> contentGroupRefs) {
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

    public Clip.Wrapper getClips() {
        // due to the whole clusterfuck where Clip descends from Content, to avoid infinite recursion
        return null;
    }

    public void setClips(Clip.Wrapper clips) {
        // due to the whole clusterfuck where Clip descends from Content, to avoid infinite recursion
    }

    public Encoding.Wrapper getEncodings() {
        return encodings;
    }

    public void setEncodings(Encoding.Wrapper encodings) {
        this.encodings = encodings;
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

    public List<RestrictionWithTimes> getRestrictions() {
        return restrictions;
    }

    public void setRestrictions(List<RestrictionWithTimes> restrictions) {
        this.restrictions = restrictions;
    }

    public String getClipOf() {
        return clipOf;
    }

    public void setClipOf(String clipOf) {
        this.clipOf = clipOf;
    }

    @Override
    public Map<String, String> getCustomFields() {
        return customFields;
    }

    @Override
    public void setCustomFields(Map<String, String> customFields) {
        this.customFields = customFields;
    }

    public static class Wrapper {

        private List<Clip> clips;

        @JsonCreator
        public Wrapper(
                @JsonProperty("clips") List<Clip> clips
        ) {
            this.clips = clips;
        }

        @JsonProperty("clips")
        public List<Clip> getClips() {
            return clips;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            Wrapper wrapper = (Wrapper) object;
            return NullOrEmptyEquality.equals(clips, wrapper.clips);
        }
    }

    public static class RestrictionWithTimes {

        private Restriction restriction;
        private UpdateTimes updateTimes;

        public RestrictionWithTimes() {}

        public RestrictionWithTimes(Restriction restriction, UpdateTimes updateTimes) {
            this.restriction = restriction;
            this.updateTimes = updateTimes;
        }

        public Restriction getRestriction() {
            return restriction;
        }

        public void setRestriction(Restriction restriction) {
            this.restriction = restriction;
        }

        public UpdateTimes getUpdateTimes() {
            return updateTimes;
        }

        public void setUpdateTimes(UpdateTimes updateTimes) {
            this.updateTimes = updateTimes;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            RestrictionWithTimes that = (RestrictionWithTimes) object;
            return Objects.equals(restriction, that.restriction) &&
                    Objects.equals(updateTimes, that.updateTimes);
        }
    }

    public static class ImageWithInterval {

        private Image image;
        private Interval interval;

        public ImageWithInterval() {}

        public ImageWithInterval(Image image, Interval interval) {
            this.image = image;
            this.interval = interval;
        }

        public Image getImage() {
            return image;
        }

        public void setImage(Image image) {
            this.image = image;
        }

        public Interval getInterval() {
            return interval;
        }

        public void setInterval(Interval interval) {
            this.interval = interval;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            ImageWithInterval that = (ImageWithInterval) object;
            return Objects.equals(image, that.image) &&
                    Objects.equals(interval, that.interval);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Clip clip = (Clip) object;
        return Objects.equals(id, clip.id) &&
                Objects.equals(canonicalUri, clip.canonicalUri) &&
                Objects.equals(curie, clip.curie) &&
                NullOrEmptyEquality.equals(aliasUrls, clip.aliasUrls) &&
                NullOrEmptyEquality.equals(aliases, clip.aliases) &&
                NullOrEmptyEquality.equals(equivalentTo, clip.equivalentTo) &&
                Objects.equals(title, clip.title) &&
                Objects.equals(shortDescription, clip.shortDescription) &&
                Objects.equals(mediumDescription, clip.mediumDescription) &&
                Objects.equals(longDescription, clip.longDescription) &&
                Objects.equals(synopses, clip.synopses) &&
                Objects.equals(description, clip.description) &&
                Objects.equals(mediaType, clip.mediaType) &&
                Objects.equals(specialization, clip.specialization) &&
                NullOrEmptyEquality.equals(genres, clip.genres) &&
                Objects.equals(publisher, clip.publisher) &&
                Objects.equals(image, clip.image) &&
                NullOrEmptyEquality.equals(images, clip.images) &&
                Objects.equals(thumbnail, clip.thumbnail) &&
                Objects.equals(scheduleOnly, clip.scheduleOnly) &&
                Objects.equals(activelyPublished, clip.activelyPublished) &&
                Objects.equals(presentationChannel, clip.presentationChannel) &&
                Objects.equals(priority, clip.priority) &&
                NullOrEmptyEquality.equals(relatedLinks, clip.relatedLinks) &&
                NullOrEmptyEquality.equals(awards, clip.awards) &&
                NullOrEmptyEquality.equals(keyPhrases, clip.keyPhrases) &&
                NullOrEmptyEquality.equals(tags, clip.tags) &&
                NullOrEmptyEquality.equals(contentGroupRefs, clip.contentGroupRefs) &&
                NullOrEmptyEquality.equals(people, clip.people) &&
                NullOrEmptyEquality.equals(languages, clip.languages) &&
                NullOrEmptyEquality.equals(certificates, clip.certificates) &&
                Objects.equals(year, clip.year) &&
                NullOrEmptyEquality.equals(manifestedAs, clip.manifestedAs) &&
                Objects.equals(genericDescription, clip.genericDescription) &&
                NullOrEmptyEquality.equals(eventRefs, clip.eventRefs) &&
                Objects.equals(containerRef, clip.containerRef) &&
                Objects.equals(isLongForm, clip.isLongForm) &&
                Objects.equals(blackAndWhite, clip.blackAndWhite) &&
                NullOrEmptyEquality.equals(countriesOfOrigin, clip.countriesOfOrigin) &&
                Objects.equals(sortKey, clip.sortKey) &&
                Objects.equals(containerSummary, clip.containerSummary) &&
                NullOrEmptyEquality.equals(broadcasts, clip.broadcasts) &&
                NullOrEmptyEquality.equals(segmentEvents, clip.segmentEvents) &&
                NullOrEmptyEquality.equals(restrictions, clip.restrictions) &&
                Objects.equals(clipOf, clip.clipOf) &&
                NullOrEmptyEquality.equals(reviews, clip.reviews) &&
                NullOrEmptyEquality.equals(ratings, clip.ratings) &&
                Objects.equals(encodings, clip.encodings) &&
                NullOrEmptyEquality.equals(customFields, clip.customFields);
    }
}
