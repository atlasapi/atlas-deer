package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;

import javax.annotation.Nullable;

public class Channel extends Identified implements Sourced {

    private final ImmutableSet<TemporalField<String>> titles;
    private final Publisher publisher;
    private final MediaType mediaType;
    private final Boolean highDefinition;
    private final Boolean regional;
    private final Boolean adult;
    private final Publisher broadcaster;
    private final ImmutableSet<Publisher> availableFrom;
    private final ImmutableSet<ChannelGroupMembership> channelGroups;
    private final ImmutableSet<String> genres;
    private final ImmutableSet<RelatedLink> relatedLinks;
    private final ImmutableSet<TemporalField<Image>> images;
    private final ChannelRef parent;
    private final ImmutableSet<ChannelRef> variations;
    private final LocalDate startDate;
    private final LocalDate endDate;

    public Channel(
            ImmutableSet<TemporalField<String>> titles,
            Publisher publisher,
            MediaType mediaType,
            Boolean highDefinition,
            Boolean regional,
            Boolean adult,
            Publisher broadcaster,
            ImmutableSet<Publisher> availableFrom,
            ImmutableSet<ChannelGroupMembership> channelGroups,
            ImmutableSet<String> genres,
            ImmutableSet<RelatedLink> relatedLinks,
            ImmutableSet<TemporalField<Image>> images,
            ChannelRef parent,
            ImmutableSet<ChannelRef> variations,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate
    ) {
        this.titles = titles;
        this.publisher = publisher;
        this.mediaType = mediaType;
        this.highDefinition = highDefinition;
        this.regional = regional;
        this.adult = adult;
        this.broadcaster = broadcaster;
        this.availableFrom = availableFrom;
        this.channelGroups = channelGroups;
        this.genres = genres;
        this.relatedLinks = relatedLinks;
        this.images = images;
        this.parent = parent;
        this.variations = variations;
        this.startDate = startDate;
        this.endDate = endDate;
    }


    @Override
    public Publisher getPublisher() {
        return this.publisher;
    }

    public ImmutableSet<TemporalField<String>> getTitles() {
        return titles;
    }

    public MediaType getMediaType() {
        return mediaType;
    }

    public Boolean getHighDefinition() {
        return highDefinition;
    }

    public Boolean getRegional() {
        return regional;
    }

    public Boolean getAdult() {
        return adult;
    }

    public Publisher getBroadcaster() {
        return broadcaster;
    }

    public ImmutableSet<Publisher> getAvailableFrom() {
        return availableFrom;
    }

    public ImmutableSet<ChannelGroupMembership> getChannelGroups() {
        return channelGroups;
    }

    public ImmutableSet<String> getGenres() {
        return genres;
    }

    public ImmutableSet<RelatedLink> getRelatedLinks() {
        return relatedLinks;
    }

    public ImmutableSet<TemporalField<Image>> getImages() {
        return images;
    }

    public ChannelRef getParent() {
        return parent;
    }

    public ImmutableSet<ChannelRef> getVariations() {
        return variations;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }
}
