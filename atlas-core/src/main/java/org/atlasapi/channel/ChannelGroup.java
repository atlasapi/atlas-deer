package org.atlasapi.channel;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.intl.Country;

import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalDate;

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

    public ChannelGroupSummary toSummary() {
        return new ChannelGroupSummary(getId(), getAliases(), getTitle(), getType());
    }
}
