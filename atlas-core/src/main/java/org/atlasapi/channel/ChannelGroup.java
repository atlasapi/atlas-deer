package org.atlasapi.channel;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.LocalDate;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroup<T extends ChannelGroupMembership> extends Identified implements Sourced {

    protected final Publisher publisher;
    protected final ImmutableSet<T> channels;
    protected final ImmutableSet<Country> availableCountries;
    protected final ImmutableSet<TemporalField<String>> titles;

    public ChannelGroup(
            Id id,
            Publisher publisher,
            Iterable<T> channels,
            Iterable<Country> availableCountries,
            Iterable<TemporalField<String>> titles
    ) {
        super(id);
        this.channels = ImmutableSet.copyOf(channels);
        this.availableCountries = ImmutableSet.copyOf(availableCountries);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
    }

    public ChannelGroup(
            Id id,
            String canonicalUri,
            Publisher publisher,
            Iterable<T> channels,
            Iterable<Country> availableCountries,
            Iterable<TemporalField<String>> titles
    ) {
        super(Identified.builder().withId(id).withCanonicalUri(canonicalUri));
        this.channels = ImmutableSet.copyOf(channels);
        this.availableCountries = ImmutableSet.copyOf(availableCountries);
        this.titles = ImmutableSet.copyOf(titles);
        this.publisher = checkNotNull(publisher);
    }


    public ChannelGroup<T> copyWithChannels(Iterable<T> channels) {
        return new ChannelGroup<>(
                getId(),
                getCanonicalUri(),
                this.publisher,
                channels,
                this.availableCountries,
                this.titles
        );
    }

    @Override
    @FieldName("source")
    public Publisher getSource() {
        return publisher;
    }

    public Set<T> getChannels() {
        return channels;
    }

    public Set<T> getChannelsAvailable(LocalDate date) {
        return channels.stream()
                .filter(ch -> ch.isAvailable(date))
                .collect(MoreCollectors.toImmutableSet());
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
