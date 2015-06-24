package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Country;
import org.atlasapi.content.Identified;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.LocalDate;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;


public class ChannelGroup<T extends ChannelGroupMembership> extends Identified implements Sourced {

    private final Publisher publisher;
    private final ImmutableSet<T> channels;
    private final ImmutableSet<Country> availableCountries;
    private final ImmutableSet<TemporalField<String>> titles;

    public ChannelGroup(
            Id id,
            Publisher publisher,
            Set<T> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles
    ) {
        super(id);
        this.channels = ImmutableSet.copyOf(channels);
        this.availableCountries = ImmutableSet.copyOf(availableCountries);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
    }

    @Override
    @FieldName("source")
    public Publisher getSource() {
        return publisher;
    }

    public Iterable<T> getChannels() {
        return channels;
    }

    public Iterable<T> getChannelsAvailable(LocalDate date) {
        return channels.stream()
                .filter(ch -> ch.isAvailable(date))
                .collect(ImmutableCollectors.toSet());
    }

    @FieldName("available_countries")
    public Set<Country> getAvailableCountries() {
        return availableCountries;
    }

    public Set<TemporalField<String>> getTitles() {
        return titles;
    }

    @FieldName("title")
    public String getTitle() {
        return TemporalField.currentOrFutureValue(titles);
    }

    public String getType() {
        return "channel_group";
    }
}
