package org.atlasapi.channel;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class Channel extends Identified implements Sourced {

    public static final String ADULT_GENRE = "http://pressassociation.com/genres/adult";

    private final ImmutableSet<TemporalField<String>> titles;
    private final Publisher publisher;
    private final MediaType mediaType;
    private final String key;
    private final Boolean highDefinition;
    private final Boolean regional;
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
    private final DateTime advertiseFrom;
    private final String shortDescription;
    private final String mediumDescription;
    private final String longDescription;
    private final String region;
    private final ImmutableSet<String> targetRegions;
    private final ChannelType channelType;

    private Channel(
            String uri,
            Id id,
            Set<Alias> aliases,
            Set<TemporalField<String>> titles,
            Publisher publisher,
            MediaType mediaType,
            String key,
            Boolean highDefinition,
            Boolean regional,
            Publisher broadcaster,
            Set<Publisher> availableFrom,
            Set<ChannelGroupMembership> channelGroups,
            Set<String> genres,
            Set<RelatedLink> relatedLinks,
            Set<TemporalField<Image>> images,
            ChannelRef parent,
            Set<ChannelRef> variations,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate,
            @Nullable DateTime advertiseFrom,
            @Nullable String shortDescription,
            @Nullable String mediumDescription,
            @Nullable String longDescription,
            @Nullable String region,
            Set<String> targetRegions,
            ChannelType channelType
    ) {
        super(uri);
        this.setAliases(aliases);
        this.setId(id);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
        this.mediaType = mediaType;
        this.key = key;
        this.highDefinition = highDefinition;
        this.regional = regional;
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
        this.advertiseFrom = advertiseFrom;
        this.shortDescription = shortDescription;
        this.mediumDescription = mediumDescription;
        this.longDescription = longDescription;
        this.region = region;
        this.targetRegions = ImmutableSet.copyOf(targetRegions);
        this.channelType = channelType;
    }

    @Override
    @FieldName("source")
    public Publisher getSource() {
        return this.publisher;
    }

    @FieldName("title")
    public String getTitle() {
        return TemporalField.currentOrFutureValue(titles);
    }

    public ImmutableSet<TemporalField<String>> getAllTitles() {
        return titles;
    }

    @FieldName("media_type")
    public MediaType getMediaType() {
        return mediaType;
    }

    @FieldName("high_definition")
    public Boolean getHighDefinition() {
        return highDefinition;
    }

    @FieldName("regional")
    public Boolean getRegional() {
        return regional;
    }

    @FieldName("broadcaster")
    public Publisher getBroadcaster() {
        return broadcaster;
    }

    @FieldName("available_from")
    public ImmutableSet<Publisher> getAvailableFrom() {
        return availableFrom;
    }

    @FieldName("channel_groups")
    public ImmutableSet<ChannelGroupMembership> getChannelGroups() {
        return channelGroups;
    }

    @FieldName("genres")
    public ImmutableSet<String> getGenres() {
        return genres;
    }

    @FieldName("related_links")
    public ImmutableSet<RelatedLink> getRelatedLinks() {
        return relatedLinks;
    }

    @FieldName("images")
    public Set<Image> getImages() {
        return TemporalField.currentValues(images);
    }

    public ImmutableSet<TemporalField<Image>> getAllImages() {
        return images;
    }

    @FieldName("parent")
    public ChannelRef getParent() {
        return parent;
    }

    @FieldName("variations")
    public ImmutableSet<ChannelRef> getVariations() {
        return variations;
    }

    @FieldName("start_date")
    public LocalDate getStartDate() {
        return startDate;
    }

    @FieldName("end_date")
    public LocalDate getEndDate() {
        return endDate;
    }

    @FieldName("advertised_from")
    public DateTime getAdvertiseFrom() {
        return advertiseFrom;
    }

    @Nullable
    @FieldName("short_description")
    public String getShortDescription() {
        return shortDescription;
    }

    @Nullable
    @FieldName("medium_description")
    public String getMediumDescription() {
        return mediumDescription;
    }

    @Nullable
    @FieldName("long_description")
    public String getLongDescription() {
        return longDescription;
    }

    @Nullable
    @FieldName("region")
    public String getRegion() {
        return region;
    }

    @FieldName("target_regions")
    public ImmutableSet<String> getTargetRegions() {
        return targetRegions;
    }

    @FieldName("type")
    public ChannelType getChannelType() {
        return channelType;
    }

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }

    public String getKey() {
        return key;
    }

    public static class Builder {

        private String uri;
        private Id id;
        private Set<Alias> aliases = Sets.newHashSet();
        private Set<TemporalField<String>> titles = Sets.newHashSet();
        private Publisher publisher;
        private MediaType mediaType;
        private String key;
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
        private DateTime advertiseFrom;
        private LocalDate startDate;
        private LocalDate endDate;
        private String shortDescription;
        private String mediumDescription;
        private String longDescription;
        private String region;
        private Set<String> targetRegions = Sets.newHashSet();
        private ChannelType channelType;

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

        public Builder withTitles(Iterable<TemporalField<String>> titles) {
            Iterables.addAll(this.titles, titles);
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
            if (parentId != null) {
                this.parent = buildChannelRef(parentId);
            }
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

        public Builder withId(Long id) {
            this.id = Id.valueOf(id);
            return this;
        }

        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public Builder withAdvertiseFrom(DateTime dateTime) {
            this.advertiseFrom = dateTime;
            return this;
        }

        public Builder withShortDescription(@Nullable String shortDescription) {
            this.shortDescription = shortDescription;
            return this;
        }

        public Builder withMediumDescription(@Nullable String mediumDescription) {
            this.mediumDescription = mediumDescription;
            return this;
        }

        public Builder withLongDescription(@Nullable String longDescription) {
            this.longDescription = longDescription;
            return this;
        }

        public Builder withRegion(@Nullable String region) {
            this.region = region;
            return this;
        }

        public Builder withTargetRegions(Iterable<String> targetRegions) {
            Iterables.addAll(this.targetRegions, targetRegions);
            return this;
        }

        public Builder withChannelType(ChannelType channelType) {
            this.channelType = channelType;
            return this;
        }

        private ChannelRef buildChannelRef(Long id) {
            return new ChannelRef(Id.valueOf(id), publisher);
        }

        public Channel build() {
            if (adult != null && adult) {
                genres.add(ADULT_GENRE);
            }
            return new Channel(
                    uri,
                    id,
                    aliases,
                    titles,
                    publisher,
                    mediaType,
                    key,
                    highDefinition,
                    regional,
                    broadcaster,
                    availableFrom,
                    channelGroups,
                    genres,
                    relatedLinks,
                    images,
                    parent,
                    variations,
                    startDate,
                    endDate,
                    advertiseFrom,
                    shortDescription,
                    mediumDescription,
                    longDescription,
                    region,
                    targetRegions,
                    channelType
            );
        }
    }
}
