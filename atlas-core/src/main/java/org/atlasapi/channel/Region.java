package org.atlasapi.channel;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class Region extends ChannelGroup<ChannelNumbering> {

    private final ChannelGroupRef platform;

    public Region(
            Publisher publisher,
            Set<ChannelNumbering> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            ChannelGroupRef platform
    ) {
        super(publisher, channels, availableCountries, titles);
        this.platform = platform;
    }

    public ChannelGroupRef getPlatform() {
        return platform;
    }

    public static Builder builder(Publisher publisher) {
        return new Builder(publisher);
    }
    public static class Builder {
        private Publisher publisher;
        private Set<ChannelNumbering> channels = Sets.newHashSet();
        private Set<Country> availableCountries= Sets.newHashSet();
        private Set<TemporalField<String>> titles = Sets.newHashSet();
        private Long platformId;

        public Builder(Publisher publisher) {
            this.publisher = checkNotNull(publisher);
        }

        public Builder withChannels(Iterable<ChannelNumbering> channels) {
            Iterables.addAll(this.channels, channels);
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

        public Builder withPlaformId(Long plaformId) {
            this.platformId = plaformId;
            return this;
        }


        public Region build() {
            ChannelGroupRef platformRef = new ChannelGroupRef(
                    Id.valueOf(platformId),
                    publisher
            );
            return new Region(
                    publisher,
                    channels,
                    availableCountries,
                    titles,
                    platformRef
            );
        }

    }
}
