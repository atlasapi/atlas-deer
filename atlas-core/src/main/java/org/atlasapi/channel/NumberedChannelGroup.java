package org.atlasapi.channel;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.stream.MoreCollectors;

import org.joda.time.LocalDate;

public abstract class NumberedChannelGroup extends ChannelGroup<ChannelNumbering> {

    private static final ChannelNumberingOrdering CHANNEL_NUMBERING_ORDERING = new ChannelNumberingOrdering();

    private static final LocalDate EARLIEST_POSSIBLE_DATE = new LocalDate(0, 1, 1);

    protected NumberedChannelGroup(Id id, Publisher publisher, Set<ChannelNumbering> channels,
            Set<Country> availableCountries, Set<TemporalField<String>> titles) {
        super(id, publisher, channels, availableCountries, titles);
    }

    protected NumberedChannelGroup(Id id, String canonicalUri, Publisher publisher, Set<ChannelNumbering> channels,
            Set<Country> availableCountries, Set<TemporalField<String>> titles) {
        super(id, canonicalUri, publisher, channels, availableCountries, titles);
    }

    @Override
    public Iterable<ChannelNumbering> getChannels() {
        return StreamSupport.stream(super.getChannels().spliterator(), false)
                .sorted(CHANNEL_NUMBERING_ORDERING)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<ChannelNumbering> getChannelsAvailable(LocalDate date, boolean lcnSharing) {
        // normally within a channel group, we expect/want only channel per channel number AKA lcn.
        // with lcnSharing = true (via annotation), we allow more than one to be served.
        if (lcnSharing) {
            return StreamSupport.stream(super.getChannelsAvailable(date, lcnSharing).spliterator(), false)
                    .sorted(CHANNEL_NUMBERING_ORDERING)
                    .collect(MoreCollectors.toImmutableList());
        }
        return StreamSupport.stream(super.getChannelsAvailable(date, lcnSharing).spliterator(), false)
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
                ).sorted(CHANNEL_NUMBERING_ORDERING).collect(MoreCollectors.toImmutableList());

    }
}
