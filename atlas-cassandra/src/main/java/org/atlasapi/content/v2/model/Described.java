package org.atlasapi.content.v2.model;

import java.util.Set;

import org.atlasapi.content.v2.model.udt.Award;
import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.content.v2.model.udt.Priority;
import org.atlasapi.content.v2.model.udt.Rating;
import org.atlasapi.content.v2.model.udt.RelatedLink;
import org.atlasapi.content.v2.model.udt.Review;
import org.atlasapi.content.v2.model.udt.Synopses;

import org.joda.time.Instant;

public interface Described extends Identified {

    String getTitle();

    void setTitle(String title);

    String getShortDescription();

    void setShortDescription(String shortDescription);

    String getMediumDescription();

    void setMediumDescription(String mediumDescription);

    String getLongDescription();

    void setLongDescription(String longDescription);

    Synopses getSynopses();

    void setSynopses(Synopses synopses);

    String getDescription();

    void setDescription(String description);

    String getMediaType();

    void setMediaType(String mediaType);

    String getSpecialization();

    void setSpecialization(String specialization);

    Set<String> getGenres();

    void setGenres(Set<String> genres);

    String getPublisher();

    void setPublisher(String publisher);

    String getImage();

    void setImage(String image);

    Set<Image> getImages();

    void setImages(Set<Image> images);

    String getThumbnail();

    void setThumbnail(String thumbnail);

    Instant getFirstSeen();

    void setFirstSeen(Instant firstSeen);

    Instant getLastFetched();

    void setLastFetched(Instant lastFetched);

    Instant getThisOrChildLastUpdated();

    void setThisOrChildLastUpdated(Instant thisOrChildLastUpdated);

    Boolean getScheduleOnly();

    void setScheduleOnly(Boolean scheduleOnly);

    Boolean getActivelyPublished();

    void setActivelyPublished(Boolean activelyPublished);

    String getPresentationChannel();

    void setPresentationChannel(String presentationChannel);

    Priority getPriority();

    void setPriority(Priority priority);

    Set<RelatedLink> getRelatedLinks();

    void setRelatedLinks(Set<RelatedLink> relatedLinks);

    Set<Award> getAwards();

    void setAwards(Set<Award> awards);

    Set<Review> getReviews();

    void setReviews(Set<Review> reviews);

    Set<Rating> getRatings();

    void setRatings(Set<Rating> ratings);
}
