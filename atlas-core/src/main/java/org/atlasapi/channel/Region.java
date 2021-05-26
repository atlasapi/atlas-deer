package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class Region extends NumberedChannelGroup {

    private final ChannelGroupRef platform;

    public Region(
            Id id,
            String canonicalUri,
            Publisher publisher,
            Set<ChannelNumbering> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            ChannelGroupRef platform,
            ChannelGroupRef channelNumbersFrom
    ) {
        super(id, canonicalUri, publisher, channels, availableCountries, titles, channelNumbersFrom);
        this.platform = platform;
    }

    public ChannelGroupRef getPlatform() {
        return platform;
    }

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }

    @Override
    public String getType() {
        return "region";
    }

    public static class Builder {

        private Id id;
        private String canonicalUri;
        private Publisher publisher;
        private Set<ChannelNumbering> channels = ImmutableSet.of();
        private Set<Country> availableCountries = Sets.newHashSet();
        private Set<TemporalField<String>> titles = Sets.newHashSet();
        private ChannelGroupRef platformRef;
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

        public Builder withPlaformId(Long platformId) {
            if (platformId != null) {
                this.platformRef = new ChannelGroupRef(
                        Id.valueOf(platformId),
                        publisher
                );
            }
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

        public Region build() {
            Region region = new Region(
                    id,
                    canonicalUri,
                    publisher,
                    channels,
                    availableCountries,
                    titles,
                    platformRef,
                    channelNumbersFromRef
            );
            region.setAliases(ImmutableSet.copyOf(aliases));
            return region;
        }

    }
}
