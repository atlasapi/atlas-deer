package org.atlasapi.channel;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Country;
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
            Set<ChannelNumbering> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            Set<ChannelGroupRef> regions
    ) {
        super(id, publisher, channels, availableCountries, titles);
        this.regions = ImmutableSet.copyOf(regions);
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
        private Publisher publisher;
        private Set<ChannelNumbering> channels = Sets.newHashSet();
        private Set<Country> availableCountries= Sets.newHashSet();
        private Set<TemporalField<String>> titles = Sets.newHashSet();
        private Set<Long> regionIds = Sets.newHashSet();

        public Builder(Publisher publisher) {
            this.publisher = checkNotNull(publisher);
        }

        public Builder withId(Long id) {
            this.id = Id.valueOf(id);
            return this;
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


        public Builder withRegionIds(Iterable<Long> regionIds) {
            Iterables.addAll(this.regionIds, regionIds);
            return this;
        }

        public Platform build() {
            HashSet<ChannelGroupRef> regions = Sets.newHashSet(
                    Collections2.transform(
                            this.regionIds,
                            new Function<Long, ChannelGroupRef>() {
                                @Override
                                public ChannelGroupRef apply(Long input) {
                                    return new ChannelGroupRef(
                                            Id.valueOf(input),
                                            publisher
                                    );
                                }
                            }
                    )
            );
            return new Platform(
                    id,
                    publisher,
                    channels,
                    availableCountries,
                    titles,
                    regions
            );
        }
    }
}
