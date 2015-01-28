package org.atlasapi.channel;

import com.metabroadcast.common.intl.Country;
import org.atlasapi.content.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;


public abstract class ChannelGroup<T extends ChannelGroupMembership> extends Identified implements Sourced {

    private final Publisher publisher;
    private final T channels;
    private final Set<Country> availableCountries;
    private final Set<TemporalField<String>> titles;


    protected ChannelGroup(
            Publisher publisher,
            T channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles
    ) {
        this.channels = channels;
        this.availableCountries = availableCountries;
        this.titles = titles;
        this.publisher = checkNotNull(publisher);
    }


    @Override
    public Publisher getPublisher() {
        return publisher;
    }

    public T getChannels() {
        return channels;
    }

    public Set<Country> getAvailableCountries() {
        return availableCountries;
    }

    public Set<TemporalField<String>> getTitles() {
        return titles;
    }
}
