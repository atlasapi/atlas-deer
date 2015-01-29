package org.atlasapi.channel;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;

import javax.annotation.Nullable;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

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
            String uri,
            Set<Alias> aliases,
            Set<TemporalField<String>> titles,
            Publisher publisher,
            MediaType mediaType,
            Boolean highDefinition,
            Boolean regional,
            Boolean adult,
            Publisher broadcaster,
            Set<Publisher> availableFrom,
            Set<ChannelGroupMembership> channelGroups,
            Set<String> genres,
            Set<RelatedLink> relatedLinks,
            Set<TemporalField<Image>> images,
            ChannelRef parent,
            Set<ChannelRef> variations,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate
    ) {
        super(checkNotNull(uri));
        this.setAliases(aliases);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
        this.mediaType = mediaType;
        this.highDefinition = highDefinition;
        this.regional = regional;
        this.adult = adult;
        this.broadcaster = broadcaster;
        this.availableFrom = ImmutableSet.copyOf(availableFrom);
        this.channelGroups = ImmutableSet.copyOf(channelGroups);
        this.genres = ImmutableSet.copyOf(genres);
        this.relatedLinks = ImmutableSet.copyOf(relatedLinks);
        this.images = ImmutableSet.copyOf(images);
        this.parent = parent;
        this.variations = ImmutableSet.copyOf(variations);
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

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }

    public static class Builder {

        private String uri;
        private Set<Alias> aliases = Sets.newHashSet();
        private Set<TemporalField<String>> titles = Sets.newHashSet();
        private Publisher publisher;
        private MediaType mediaType;
        private Boolean highDefinition;
        private Boolean regional;
        private Boolean adult;
        private Publisher broadcaster;
        private Set<Publisher> availableFrom = Sets.newHashSet();
        private Set<ChannelGroupMembership> channelGroups = Sets.newHashSet();
        private Set<String> genres = Sets.newHashSet();
        private Set<RelatedLink> relatedLinks = Sets.newHashSet();
        private Set<TemporalField<Image>> images = Sets.newHashSet();
        private ChannelRef parent;
        private Set<ChannelRef> variations = Sets.newHashSet();

        private LocalDate startDate;
        private LocalDate endDate;

        public Builder(Publisher publisher) {
            this.publisher = checkNotNull(publisher);
        }

        public Builder withUri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder withAliases(Iterable<Alias> aliases) {
            Iterables.addAll(this.aliases, aliases);
            return this;
        }

        public Builder withTitles(TemporalField<String> title) {
            this.titles.add(title);
            return this;
        }

        public Builder withMediaType(MediaType mediaType) {
            this.mediaType = mediaType;
            return this;
        }

        public Builder withHighDefinition(Boolean highDefinition) {
            this.highDefinition = highDefinition;
            return this;
        }

        public Builder withRegional(Boolean regional) {
            this.regional = regional;
            return this;
        }

        public Builder withAdult(Boolean adult) {
            this.adult = adult;
            return this;
        }

        public Builder withBroadcaster(Publisher broadcaster) {
            this.broadcaster = broadcaster;
            return this;
        }

        public Builder withAvailableFrom(Publisher availableFrom) {
            this.availableFrom.add(availableFrom);
            return this;
        }

        public Builder withAvailableFrom(Iterable<Publisher> availableFrom) {
            Iterables.addAll(this.availableFrom, availableFrom);
            return this;
        }

        public Builder withChannelGroups(Iterable<ChannelGroupMembership> channelGroups) {
            Iterables.addAll(this.channelGroups, channelGroups);
            return this;
        }

        public Builder withGenre(String genre) {
            this.genres.add(genre);
            return this;
        }

        public Builder withGenres(Iterable<String> genres) {
            Iterables.addAll(this.genres, genres);
            return this;
        }

        public Builder withRelatedLink(RelatedLink relatedLink) {
            this.relatedLinks.add(relatedLink);
            return this;
        }

        public Builder withRelatedLinks(Iterable<RelatedLink> relatedLinks) {
            Iterables.addAll(this.relatedLinks, relatedLinks);
            return this;
        }

        public Builder withImages(Iterable<TemporalField<Image>> images) {
            Iterables.addAll(this.images, images);
            return this;
        }

        public Builder withParent(Long parentId) {
            this.parent = buildChannelRef(parentId);
            return this;
        }

        public Builder withVariations(Iterable<Long> variations) {

            Iterables.addAll(
                    this.variations,
                    Iterables.transform(
                            variations,
                            new Function<Long, ChannelRef>() {
                                @Override
                                public ChannelRef apply(Long input) {
                                    return buildChannelRef(input);
                                }
                            }
                    )
            );
            return this;
        }

        public Builder withStartDate(LocalDate startDate) {
            this.startDate = startDate;
            return this;
        }

        public Builder withEndDate(LocalDate endDate) {
            this.endDate = endDate;
            return this;
        }


        private ChannelRef buildChannelRef(Long id) {
            return new ChannelRef(Id.valueOf(id), publisher);
        }

        public Channel build() {
            return new Channel(
                    uri,
                    aliases,
                    titles,
                    publisher,
                    mediaType,
                    highDefinition,
                    regional,
                    adult,
                    broadcaster,
                    availableFrom,
                    channelGroups,
                    genres,
                    relatedLinks,
                    images,
                    parent,
                    variations,
                    startDate,
                    endDate
            );
        }
    }
}
