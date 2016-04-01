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
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.RelatedLink;
import org.atlasapi.content.v2.model.udt.ReleaseDate;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.model.udt.Synopses;
import org.atlasapi.content.v2.model.udt.Tag;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.joda.time.Instant;

@Table(name = "content_v2")
public class Content implements ContentIface {

    // TODO: make this an enum once we figure out the DatastaxCassandraService nonsense
    public static final String ROW_CLIPS = "clips";
    public static final String ROW_ENCODINGS = "encodings";
    public static final String ROW_MAIN = "main";

    @PartitionKey
    @Column(name = "id")
    private Long id;

    @ClusteringColumn
    @Column(name = "dcr")
    private String discriminator;

    @Column(name = "t")
    private String type;

    @Column(name = "c")
    private String canonicalUri;

    @Column(name = "cu")
    private String curie;

    @Column(name = "au")
    private Set<String> aliasUrls;

    @FrozenValue
    @Column(name = "a")
    private Set<Alias> aliases;

    @FrozenValue
    @Column(name = "e")
    private Set<Ref> equivalentTo;

    @Column(name = "lu")
    private Instant lastUpdated;

    @Column(name = "eu")
    private Instant equivalenceUpdate;

    @Column(name = "tt")
    private String title;

    @Column(name = "sd")
    private String shortDescription;

    @Column(name = "md")
    private String mediumDescription;

    @Column(name = "ld")
    private String longDescription;

    @Frozen
    @Column(name = "sn")
    private Synopses synopses;

    @Column(name = "ds")
    private String description;

    @Column(name = "mt")
    private String mediaType;

    @Column(name = "sp")
    private String specialization;

    @Column(name = "g")
    private Set<String> genres;

    @Column(name = "pb")
    private String publisher;

    @Column(name = "im")
    private String image;

    @FrozenValue
    @Column(name = "ims")
    private Set<Image> images;

    @Column(name = "th")
    private String thumbnail;

    @Column(name = "fs")
    private Instant firstSeen;

    @Column(name = "lf")
    private Instant lastFetched;

    @Column(name = "toclu")
    private Instant thisOrChildLastUpdated;

    @Column(name = "so")
    private Boolean scheduleOnly;

    @Column(name = "ap")
    private Boolean activelyPublished;

    @Column(name = "pc")
    private String presentationChannel;

    @Frozen
    @Column(name = "pr")
    private Priority priority;

    @FrozenValue
    @Column(name = "rl")
    private Set<RelatedLink> relatedLinks;

    @FrozenValue
    @Column(name = "aw")
    private Set<Award> awards;

    @FrozenValue
    @Column(name = "kp")
    private Set<KeyPhrase> keyPhrases;

    @FrozenValue
    @Column(name = "tg")
    private List<Tag> tags;

    @FrozenValue
    @Column(name = "cgr")
    private List<ContentGroupRef> contentGroupRefs;

    @FrozenValue
    @Column(name = "pp")
    private List<CrewMember> people;

    @Column(name = "ln")
    private Set<String> languages;

    @FrozenValue
    @Column(name = "cr")
    private Set<Certificate> certificates;

    @Column(name = "yr")
    private Integer year;

    @Column(name = "gd")
    private Boolean genericDescription;

    @FrozenValue
    @Column(name = "er")
    private Set<Ref> eventRefs;

    @Column(name = "isrc")
    private String isrc;

    @Column(name = "dr")
    private Long duration;

    @Column(name = "snb")
    private Integer seriesNumber;

    @Column(name = "te")
    private Integer totalEpisodes;

    @Frozen
    @Column(name = "br")
    private Ref brandRef;

    @Frozen
    @Column(name = "cnr")
    private ContainerRef containerRef;

    @Column(name = "ilf")
    private Boolean isLongForm;

    @Column(name = "bnw")
    private Boolean blackAndWhite;

    @Column(name = "coo")
    private Set<String> countriesOfOrigin;

    @Column(name = "sk")
    private String sortKey;

    @Frozen
    @Column(name = "cns")
    private ContainerSummary containerSummary;

    @FrozenValue
    @Column(name = "bc")
    private Set<Broadcast> broadcasts;

    @FrozenValue
    @Column(name = "sev")
    private List<SegmentEvent> segmentEvents;

    @FrozenValue
    @Column(name = "rr")
    private Set<Restriction> restrictions;

    @Column(name = "wu")
    private String websiteUrl;

    @Column(name = "sub")
    private Set<String> subtitles;

    @FrozenValue
    @Column(name = "rd")
    private Set<ReleaseDate> releaseDates;

    @Column(name = "en")
    private Integer episodeNumber;

    @Column(name = "pn")
    private Integer partNumber;

    @Column(name = "spc")
    private Boolean special;

    @FrozenValue
    @Column(name = "ser")
    private List<SeriesRef> seriesRefs;

    @FrozenValue
    @Column(name = "itr")
    private List<ItemRef> itemRefs;

    @Frozen("map<frozen<ItemRef>, frozen<list<BroadcastRef>>>")
    @Column(name = "upc")
    private Map<ItemRef, List<BroadcastRef>> upcomingContent;

    @Frozen("map<frozen<ItemRef>, frozen<list<LocationSummary>>>")
    @Column(name = "avc")
    private Map<ItemRef, List<LocationSummary>> availableContent;

    @FrozenValue
    @Column(name = "its")
    private List<ItemSummary> itemSummaries;

    @Column(name = "bl")
    private String jsonBlob;

    public String getJsonBlob() {
        return jsonBlob;
    }

    public void setJsonBlob(String jsonBlob) {
        this.jsonBlob = jsonBlob;
    }

    public String getDiscriminator() {
        return discriminator;
    }

    public void setDiscriminator(String discriminator) {
        this.discriminator = discriminator;
    }

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
    public List<ContentGroupRef> getContentGroupRefs() {
        return contentGroupRefs;
    }

    @Override
    public void setContentGroupRefs(
            List<ContentGroupRef> contentGroupRefs) {
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
