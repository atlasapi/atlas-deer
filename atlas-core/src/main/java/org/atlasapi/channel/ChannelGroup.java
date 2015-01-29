package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.content.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;


public abstract class ChannelGroup<T extends ChannelGroupMembership> extends Identified implements Sourced {

    private final Publisher publisher;
    private final ImmutableSet<T> channels;
    private final ImmutableSet<Country> availableCountries;
    private final ImmutableSet<TemporalField<String>> titles;


    protected ChannelGroup(
            Publisher publisher,
            Set<T> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles
    ) {
        this.channels = ImmutableSet.copyOf(channels);
        this.availableCountries = ImmutableSet.copyOf(availableCountries);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
    }


    @Override
    public Publisher getPublisher() {
        return publisher;
    }

    public Set<T> getChannels() {
        return channels;
    }

    public Set<Country> getAvailableCountries() {
        return availableCountries;
    }

    public Set<TemporalField<String>> getTitles() {
        return titles;
    }
}
