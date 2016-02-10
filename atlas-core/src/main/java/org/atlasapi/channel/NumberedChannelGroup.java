package org.atlasapi.channel;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.intl.Country;

import org.joda.time.LocalDate;

public abstract class NumberedChannelGroup extends ChannelGroup<ChannelNumbering> {

    private static final ChannelNumberingOrdering CHANNEL_NUMBERING_ORDERING = new ChannelNumberingOrdering();

    private static final LocalDate EARLIEST_POSSIBLE_DATE = new LocalDate(0, 1, 1);

    protected NumberedChannelGroup(Id id, Publisher publisher, Set<ChannelNumbering> channels,
            Set<Country> availableCountries, Set<TemporalField<String>> titles) {
        super(id, publisher, channels, availableCountries, titles);
    }

    @Override
    public Iterable<ChannelNumbering> getChannels() {
        return StreamSupport.stream(super.getChannels().spliterator(), false)
                .sorted(CHANNEL_NUMBERING_ORDERING)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<ChannelNumbering> getChannelsAvailable(LocalDate date) {
        return StreamSupport.stream(super.getChannelsAvailable(date).spliterator(), false)
                //we need to use randomUUID in order to avoid deduplicating chanels which have no numbering
                .collect(Collectors.groupingBy(cn -> cn.getChannelNumber()
                        .orElse(UUID.randomUUID().toString())))
                .values()
                .stream()
                .map(
                        channelNumberings ->
                                channelNumberings.stream()
                                        .sorted(
                                                (o1, o2) -> -o1.getStartDate()
                                                        .orElse(EARLIEST_POSSIBLE_DATE)
                                                        .compareTo(o2.getStartDate()
                                                                .orElse(EARLIEST_POSSIBLE_DATE)))
                                        .findFirst().get()
                ).sorted(CHANNEL_NUMBERING_ORDERING).collect(ImmutableCollectors.toList());

    }
}
