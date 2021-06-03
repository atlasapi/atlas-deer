package org.atlasapi.channel;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class Platform extends NumberedChannelGroup {

    private final ImmutableSet<ChannelGroupRef> regions;

    public Platform(
            Id id,
            Publisher publisher,
            Iterable<ChannelNumbering> channels,
            Iterable<Country> availableCountries,
            Iterable<TemporalField<String>> titles,
            Iterable<ChannelGroupRef> regions,
            ChannelGroupRef channelNumbersFrom
    ) {
        super(id, publisher, channels, availableCountries, titles, channelNumbersFrom);
        this.regions = ImmutableSet.copyOf(regions);
    }

    public Platform(
            Id id,
            String canonicalUri,
            Publisher publisher,
            Iterable<ChannelNumbering> channels,
            Iterable<Country> availableCountries,
            Iterable<TemporalField<String>> titles,
            Iterable<ChannelGroupRef> regions,
            ChannelGroupRef channelNumbersFrom
    ) {
        super(id, canonicalUri, publisher, channels, availableCountries, titles, channelNumbersFrom);
        this.regions = ImmutableSet.copyOf(regions);
    }

    @Override
    public ChannelGroup<ChannelNumbering> copyWithChannels(Iterable<ChannelNumbering> channels) {
        return new Platform(
                getId(),
                getCanonicalUri(),
                this.publisher,
                channels,
                this.availableCountries,
                this.titles,
                this.regions,
                this.channelNumbersFrom
        );
    }

    public Set<ChannelGroupRef> getRegions() {
        return regions;
    }

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }

    @Override
    public String getType() {
        return "platform";
    }

    public static class Builder {

        private Id id;
        private String canonicalUri;
        private Publisher publisher;
        private Set<ChannelNumbering> channels = ImmutableSet.of();
        private Set<Country> availableCountries = Sets.newHashSet();
        private Set<TemporalField<String>> titles = Sets.newHashSet();
        private Set<Long> regionIds = Sets.newHashSet();
        private Set<Alias> aliases = Sets.newHashSet();
        private ChannelGroupRef channelNumbersFromRef;

        public Builder(Publisher publisher) {
            this.publisher = checkNotNull(publisher);
        }

        public Builder withId(Long id) {
            this.id = Id.valueOf(id);
            return this;
        }

        public Builder withCanonicalUri(String canonicalUri) {
            this.canonicalUri = canonicalUri;
            return this;
        }

        public Builder withChannels(Iterable<ChannelNumbering> channels) {
            // original order needs to be preserved since it is a supported feature
            this.channels = ImmutableSet.<ChannelNumbering>builder()
                    .addAll(this.channels)
                    .addAll(channels)
                    .build();
            return this;
        }

        public Builder withAvailableCountries(Iterable<Country> availableCountries) {
            Iterables.addAll(this.availableCountries, availableCountries);
            return this;
        }

        public Builder withTitles(Iterable<TemporalField<String>> titles) {
            Iterables.addAll(this.titles, titles);
            return this;
        }

        public Builder withRegionIds(Iterable<Long> regionIds) {
            Iterables.addAll(this.regionIds, regionIds);
            return this;
        }

        public Builder withAliases(Iterable<Alias> aliases) {
            Iterables.addAll(this.aliases, aliases);
            return this;
        }

        public Builder withChannelNumbersFromId(Long channelNumbersFromId) {
            if (channelNumbersFromId != null) {
                this.channelNumbersFromRef = new ChannelGroupRef(
                        Id.valueOf(channelNumbersFromId),
                        publisher
                );
            }
            return this;
        }

        public Platform build() {
            HashSet<ChannelGroupRef> regions = Sets.newHashSet(
                    Collections2.transform(
                            this.regionIds,
                            input -> new ChannelGroupRef(
                                    Id.valueOf(input),
                                    publisher
                            )
                    )
            );
            Platform platform = new Platform(
                    id,
                    canonicalUri,
                    publisher,
                    channels,
                    availableCountries,
                    titles,
                    regions,
                    channelNumbersFromRef
            );
            platform.setAliases(ImmutableSet.copyOf(aliases));

            return platform;
        }
    }
}
