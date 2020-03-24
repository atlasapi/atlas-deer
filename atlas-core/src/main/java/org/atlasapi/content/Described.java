/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.entity.Award;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.Rating;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Optional.ofNullable;

public abstract class Described extends Identified implements Sourced {

    private String title;
    private Set<LocalizedTitle> localizedTitles = ImmutableSet.of();

    private String shortDescription;
    private String mediumDescription;
    private String longDescription;

    private Synopses synopses;

    private String description;

    private MediaType mediaType = MediaType.VIDEO;
    private Specialization specialization;

    private ImmutableSet<String> genres = ImmutableSet.of();

    protected Publisher publisher;
    private String image;
    private Set<Image> images = ImmutableSet.of();
    private String thumbnail;

    private DateTime firstSeen;
    private DateTime lastFetched;
    private DateTime thisOrChildLastUpdated;
    private boolean scheduleOnly = false;
    private boolean activelyPublished = true;

    private String presentationChannel;

    private Priority priority;

    protected Set<RelatedLink> relatedLinks = ImmutableSet.of();

    private Set<Review> reviews = ImmutableSet.of();
    private Set<Rating> ratings = ImmutableSet.of();

    private Set<Award> awards = ImmutableSet.of();

    public Described(String uri, String curie, Publisher publisher) {
        super(uri, curie);
        this.publisher = publisher;
    }

    public Described(String uri, String curie) {
        this(uri, curie, null);
    }

    public Described(String uri) {
        super(uri);
    }

    public Described() { /* some legacy code still requires a default constructor */ }

    public Described(Id id, Publisher source) {
        super(id);
        this.publisher = source;
    }

    @FieldName("last_fetched")
    public DateTime getLastFetched() {
        return lastFetched;
    }

    public void setLastFetched(DateTime lastFetched) {
        this.lastFetched = lastFetched;
    }

    @FieldName("first_seen")
    public DateTime getFirstSeen() {
        return this.firstSeen;
    }

    public void setFirstSeen(DateTime firstSeen) {
        this.firstSeen = firstSeen;
    }

    public void setGenres(Iterable<String> genres) {
        this.genres = ImmutableSet.copyOf(genres);
    }

    @FieldName("genres")
    public Set<String> getGenres() {
        return this.genres;
    }

    @FieldName("priority")
    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    @FieldName("title")
    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @FieldName("localized_titles")
    public Set<LocalizedTitle> getLocalizedTitles() {
        return localizedTitles;
    }

    public void setLocalizedTitles(Set<LocalizedTitle> localizedTitles) {
        this.localizedTitles = ImmutableSet.copyOf(localizedTitles);
    }

    @FieldName("synopses")
    public Synopses getSynopses() {
        return this.synopses;
    }

    @FieldName("description")
    public String getDescription() {
        return this.description;
    }

    public void setSynopses(Synopses synopses) {
        this.synopses = synopses;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @FieldName("short_description")
    public String getShortDescription() {
        return this.shortDescription;
    }

    @FieldName("medium_description")
    public String getMediumDescription() {
        return this.mediumDescription;
    }

    @FieldName("long_description")
    public String getLongDescription() {
        return this.longDescription;
    }

    public void setShortDescription(String shortDescription) {
        this.shortDescription = shortDescription;
    }

    public void setMediumDescription(String mediumDescription) {
        this.mediumDescription = mediumDescription;
    }

    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

    @FieldName("publisher")
    @Override
    public Publisher getSource() {
        return publisher;
    }

    public void setPublisher(Publisher publisher) {
        this.publisher = publisher;
    }

    @FieldName("image")
    public String getImage() {
        return image;
    }

    @FieldName("thumbnail")
    public String getThumbnail() {
        return thumbnail;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    @FieldName("this_or_child_last_updated")
    public DateTime getThisOrChildLastUpdated() {
        return thisOrChildLastUpdated;
    }

    public void setThisOrChildLastUpdated(DateTime thisOrChildLastUpdated) {
        this.thisOrChildLastUpdated = thisOrChildLastUpdated;
    }

    public boolean isActivelyPublished() {
        return activelyPublished;
    }

    public void setActivelyPublished(boolean activelyPublished) {
        this.activelyPublished = activelyPublished;
    }

    public void setMediaType(MediaType mediaType) {
        this.mediaType = mediaType;
    }

    @FieldName("media_type")
    public MediaType getMediaType() {
        return this.mediaType;
    }

    @FieldName("specialization")
    public Specialization getSpecialization() {
        return specialization;
    }

    public void setSpecialization(Specialization specialization) {
        this.specialization = specialization;
    }

    public void setScheduleOnly(boolean scheduleOnly) {
        this.scheduleOnly = scheduleOnly;
    }

    @FieldName("schedule_only")
    public boolean isScheduleOnly() {
        return scheduleOnly;
    }

    public void setPresentationChannel(Channel channel) {
        setPresentationChannel(channel.getKey());
    }

    public void setPresentationChannel(String channel) {
        this.presentationChannel = channel;
    }

    @FieldName("presentation_channel")
    public String getPresentationChannel() {
        return this.presentationChannel;
    }

    public void setImages(Iterable<Image> images) {
        this.images = ImmutableSet.copyOf(images);
    }

    @FieldName("images")
    public Set<Image> getImages() {
        return images;
    }

    @FieldName("primary_image")
    public Image getPrimaryImage() {
        return Iterables.getOnlyElement(Iterables.filter(images, Image.IS_PRIMARY), null);
    }

    @FieldName("related_links")
    public Set<RelatedLink> getRelatedLinks() {
        return relatedLinks;
    }

    public void setRelatedLinks(Iterable<RelatedLink> links) {
        relatedLinks = ImmutableSet.copyOf(links);
    }

    public void addRelatedLink(RelatedLink link) {
        relatedLinks = ImmutableSet.<RelatedLink>builder().add(link).addAll(relatedLinks).build();
    }

    @FieldName("reviews")
    public Set<Review> getReviews() {
        return reviews;
    }

    public void setReviews(Iterable<Review> reviews) {
        this.reviews = ImmutableSet.copyOf(reviews);
    }

    @FieldName("ratings")
    public Set<Rating> getRatings() {
        return ratings;
    }

    public void setRatings(Iterable<Rating> ratings) {
        this.ratings = ImmutableSet.copyOf(ratings);
    }

    @FieldName("awards")
    public Set<Award> getAwards() {
        return awards;
    }

    public void setAwards(Set<Award> awards) {
        this.awards = ImmutableSet.copyOf(awards);
    }

    public static Described copyTo(Described from, Described to) {
        Identified.copyTo(from, to);
        to.description = from.description;
        to.firstSeen = from.firstSeen;
        to.genres = ImmutableSet.copyOf(from.genres);
        to.image = from.image;
        to.lastFetched = from.lastFetched;
        to.mediaType = from.mediaType;
        to.publisher = from.publisher;
        to.specialization = from.specialization;
        to.thisOrChildLastUpdated = from.thisOrChildLastUpdated;
        to.thumbnail = from.thumbnail;
        to.title = from.title;
        to.localizedTitles = from.localizedTitles;
        to.scheduleOnly = from.scheduleOnly;
        to.presentationChannel = from.presentationChannel;
        to.images = from.images;
        to.shortDescription = from.shortDescription;
        to.mediumDescription = from.mediumDescription;
        to.longDescription = from.longDescription;
        to.activelyPublished = from.activelyPublished;
        to.reviews = from.reviews;
        to.ratings = from.ratings;
        to.awards = from.awards;
        return to;
    }


    /**
     * Same as above except would prefer any value over nulls when copying
     * Needed in the case of barb overrides as they overwrite their equivs
     * data with nulls.
     */
    public static Described copyToPreferNonNull(Described from, Described to) {
        Identified.copyToPreferNonNull(from, to);
        to.description = isNullOrEmpty(from.description) ? to.description : from.description;
        to.firstSeen = ofNullable(from.firstSeen).orElse(to.firstSeen);
        to.genres = from.genres.isEmpty() ? to.genres : ImmutableSet.copyOf(from.genres);
        to.image = isNullOrEmpty(from.image) ? to.image : from.image;
        to.lastFetched = ofNullable(from.lastFetched).orElse(to.lastFetched);
        to.mediaType = ofNullable(from.mediaType).orElse(to.mediaType);
        to.publisher = ofNullable(from.publisher).orElse(to.publisher);
        to.specialization = ofNullable(from.specialization).orElse(to.specialization);
        to.thisOrChildLastUpdated = ofNullable(from.thisOrChildLastUpdated).orElse(to.thisOrChildLastUpdated);
        to.thumbnail = isNullOrEmpty(from.thumbnail) ? to.thumbnail : from.thumbnail;
        to.title = isNullOrEmpty(from.title) ? to.title : from.title;
        to.localizedTitles = from.localizedTitles.isEmpty() ? to.localizedTitles
                                                            : from.localizedTitles;
        to.scheduleOnly = from.scheduleOnly;
        to.presentationChannel = isNullOrEmpty(from.presentationChannel) ? to.presentationChannel : from.presentationChannel;
        to.images = from.images.isEmpty() ? to.images : from.images;
        to.shortDescription = isNullOrEmpty(from.shortDescription) ? to.shortDescription : from.shortDescription;
        to.mediumDescription = isNullOrEmpty(from.mediumDescription) ? to.mediumDescription : from.mediumDescription;
        to.longDescription = isNullOrEmpty(from.longDescription) ? to.longDescription : from.longDescription;
        to.activelyPublished = from.activelyPublished;
        to.reviews = from.reviews.isEmpty() ? to.reviews : from.reviews;
        to.ratings = from.ratings.isEmpty() ? to.ratings : from.ratings;
        to.awards = from.awards.isEmpty() ? to.awards : from.awards;
        return to;
    }

    public <T extends Described> T copyTo(T to) {
        copyTo(this, to);
        return to;
    }

    public <T extends Described> T copyToPreferNonNull(T to) {
        copyToPreferNonNull(this, to);
        return to;
    }

    public abstract Described copy();

    public <T extends Described> boolean isEquivalentTo(T content) {
        return getEquivalentTo().contains(EquivalenceRef.valueOf(content))
                || Iterables.contains(
                        Iterables.transform(
                                content.getEquivalentTo(),
                                Identifiables.toId()
                        ),
                        getId()
                );
    }

}
