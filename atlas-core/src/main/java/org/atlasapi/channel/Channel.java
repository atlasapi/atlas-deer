package org.atlasapi.channel;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
    private final Boolean timeshifted;
    private final Boolean regional;
    private final Publisher broadcaster;
    private final ImmutableSet<Publisher> availableFrom;
    private final ImmutableSet<ChannelGroupMembership> channelGroups;
    private final ImmutableSet<String> genres;
    private final ImmutableSet<RelatedLink> relatedLinks;
    private final ImmutableSet<TemporalField<Image>> images;
    private final ChannelRef parent;
    private final ImmutableSet<ChannelRef> variations;
    private final ImmutableSet<ChannelEquivRef> sameAs;
    private final LocalDate startDate;
    private final LocalDate endDate;
    private final DateTime advertiseFrom;
    private final DateTime advertiseTo;
    private final String shortDescription;
    private final String mediumDescription;
    private final String longDescription;
    private final String region;
    private final ImmutableSet<String> targetRegions;
    private final ChannelType channelType;
    private final Boolean interactive;

    private Channel(
            String uri,
            Id id,
            Set<Alias> aliases,
            Set<TemporalField<String>> titles,
            Publisher publisher,
            MediaType mediaType,
            String key,
            Boolean highDefinition,
            Boolean timeshifted,
            Boolean regional,
            Publisher broadcaster,
            Set<Publisher> availableFrom,
            Set<ChannelGroupMembership> channelGroups,
            Set<String> genres,
            Set<RelatedLink> relatedLinks,
            Set<TemporalField<Image>> images,
            ChannelRef parent,
            Set<ChannelRef> variations,
            Set<ChannelEquivRef> sameAs,
            @Nullable LocalDate startDate,
            @Nullable LocalDate endDate,
            @Nullable DateTime advertiseFrom,
            @Nullable DateTime advertiseTo,
            @Nullable String shortDescription,
            @Nullable String mediumDescription,
            @Nullable String longDescription,
            @Nullable String region,
            Set<String> targetRegions,
            ChannelType channelType,
            Boolean interactive
    ) {
        super(uri);
        this.setAliases(aliases);
        this.setId(id);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
        this.mediaType = mediaType;
        this.key = key;
        this.highDefinition = highDefinition;
        this.timeshifted = timeshifted;
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
        this.advertiseTo = advertiseTo;
        this.shortDescription = shortDescription;
        this.mediumDescription = mediumDescription;
        this.longDescription = longDescription;
        this.region = region;
        this.targetRegions = ImmutableSet.copyOf(targetRegions);
        this.channelType = channelType;
        this.interactive = interactive;
        this.sameAs = ImmutableSet.copyOf(sameAs);
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

    @FieldName("timeshifted")
    public Boolean getTimeshifted() {
        return timeshifted;
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

    @FieldName("same_as")
    public ImmutableSet<ChannelEquivRef> getSameAs() {
        return sameAs;
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

    @FieldName("advertised_to")
    public DateTime getAdvertiseTo() {
        return advertiseTo;
    }

    public ChannelRef toRef() {
        return new ChannelRef(getId(), getSource());
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

    @FieldName("interactive")
    public Boolean getInteractive() {
        return interactive;
    }

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }

    public static Builder builderFrom(Channel channel) {
        return new Builder(channel.getSource()).copyOf(channel);
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
        private Boolean timeshifted;
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
        private Set<ChannelEquivRef> sameAs = Sets.newHashSet();
        private DateTime advertiseFrom;
        private DateTime advertiseTo;
        private LocalDate startDate;
        private LocalDate endDate;
        private String shortDescription;
        private String mediumDescription;
        private String longDescription;
        private String region;
        private Set<String> targetRegions = Sets.newHashSet();
        private ChannelType channelType = ChannelType.CHANNEL;
        private Boolean interactive = false;

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

        public Builder withAlias(Alias alias) {
            this.aliases.add(alias);
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

        public Builder withTimeshifted(Boolean timeshifted) {
            this.timeshifted = timeshifted;
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
                    StreamSupport.stream(variations.spliterator(), false)
                            .map(this::buildChannelRef)
                            .collect(Collectors.toList())
            );
            return this;
        }

        public Builder withSameAs(Set<ChannelEquivRef> sameAs) {
            this.sameAs = ImmutableSet.copyOf(sameAs);
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

        public Builder withAdvertiseTo(DateTime dateTime) {
            this.advertiseTo = dateTime;
            return this;
        }

        public Builder copyOf(Channel channel) {
            this.uri = channel.getCanonicalUri();
            this.id = channel.getId();
            this.aliases = Sets.newHashSet(channel.getAliases());
            this.titles = Sets.newHashSet(channel.getAllTitles());
            this.publisher = channel.getSource();
            this.mediaType = channel.getMediaType();
            this.key = channel.getKey();
            this.highDefinition = channel.getHighDefinition();
            this.timeshifted = channel.getTimeshifted();
            this.regional = channel.getRegional();
            this.broadcaster = channel.getBroadcaster();
            this.availableFrom = channel.availableFrom;
            this.channelGroups = Sets.newHashSet(channel.getChannelGroups());
            this.genres = Sets.newHashSet(channel.getGenres());
            this.relatedLinks = Sets.newHashSet(channel.getRelatedLinks());
            this.images = Sets.newHashSet(channel.getAllImages());
            this.parent = channel.getParent();
            this.variations = Sets.newHashSet(channel.getVariations());
            this.sameAs = Sets.newHashSet(channel.getSameAs());
            this.startDate = channel.getStartDate();
            this.endDate = channel.getEndDate();
            this.advertiseFrom = channel.getAdvertiseFrom();
            this.advertiseTo = channel.getAdvertiseTo();

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

        public Builder withInteractive(Boolean interactive) {
            this.interactive = interactive;
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
                    timeshifted,
                    regional,
                    broadcaster,
                    availableFrom,
                    channelGroups,
                    genres,
                    relatedLinks,
                    images,
                    parent,
                    variations,
                    sameAs,
                    startDate,
                    endDate,
                    advertiseFrom,
                    advertiseTo,
                    shortDescription,
                    mediumDescription,
                    longDescription,
                    region,
                    targetRegions,
                    channelType,
                    interactive
            );
        }
    }
}
