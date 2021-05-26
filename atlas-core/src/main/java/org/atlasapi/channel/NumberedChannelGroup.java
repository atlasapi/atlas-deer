package org.atlasapi.channel;

import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class NumberedChannelGroup extends ChannelGroup<ChannelNumbering> {

    private static final ChannelNumberingOrdering CHANNEL_NUMBERING_ORDERING = new ChannelNumberingOrdering();

    private static final LocalDate EARLIEST_POSSIBLE_DATE = new LocalDate(0, 1, 1);

    private static final Comparator<ChannelNumbering> MOST_RECENT_START_DATE_COMPARATOR = (numbering1, numbering2) ->
            -numbering1.getStartDate().orElse(EARLIEST_POSSIBLE_DATE)
                    .compareTo(
                            numbering2.getStartDate().orElse(EARLIEST_POSSIBLE_DATE)
                    );

    private final ChannelGroupRef channelNumbersFrom;

    public enum ChannelOrdering {
        SPECIFIED("specified"),
        CHANNEL_NUMBER("channel_number"),
        ;

        private static final Map<String, ChannelOrdering> MAP_BY_NAME = Arrays.stream(ChannelOrdering.values())
                .collect(MoreCollectors.toImmutableMap(ChannelOrdering::getName, ordering -> ordering));

        @Nullable
        public static ChannelOrdering forName(String name) {
            return MAP_BY_NAME.get(name);
        }

        public static Set<String> names() {
            return MAP_BY_NAME.keySet();
        }

        private final String name;
        ChannelOrdering(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return getName();
        }
    }

    protected NumberedChannelGroup(
            Id id,
            Publisher publisher,
            Set<ChannelNumbering> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            @Nullable ChannelGroupRef channelNumbersFrom
    ) {
        super(id, publisher, channels, availableCountries, titles);
        this.channelNumbersFrom = channelNumbersFrom;
    }

    protected NumberedChannelGroup(
            Id id,
            String canonicalUri,
            Publisher publisher,
            Set<ChannelNumbering> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            @Nullable ChannelGroupRef channelNumbersFrom
    ) {
        super(id, canonicalUri, publisher, channels, availableCountries, titles);
        this.channelNumbersFrom = channelNumbersFrom;
    }

    public Optional<ChannelGroupRef> getChannelNumbersFrom() {
        return Optional.ofNullable(channelNumbersFrom);
    }

    @Override
    public Iterable<ChannelNumbering> getChannels() {
        return getChannels(ChannelOrdering.CHANNEL_NUMBER);
    }

    public Iterable<ChannelNumbering> getChannels(ChannelOrdering ordering) {
        switch (ordering) {
            case SPECIFIED:
                return super.getChannels();
            case CHANNEL_NUMBER:
                return StreamSupport.stream(super.getChannels().spliterator(), false)
                        .sorted(CHANNEL_NUMBERING_ORDERING)
                        .collect(MoreCollectors.toImmutableSet());
            default:
                throw new IllegalArgumentException("Unsupported channel ordering: " + ordering);
        }
    }

    @Override
    public Iterable<ChannelNumbering> getChannelsAvailable(LocalDate date, boolean lcnSharing) {
        return getChannelsAvailable(date, ChannelOrdering.CHANNEL_NUMBER, lcnSharing);
    }

    public Iterable<ChannelNumbering> getChannelsAvailable(
            LocalDate date,
            ChannelOrdering ordering,
            boolean lcnSharing
    ) {
        // normally within a channel group, we expect/want only channel per channel number AKA lcn.
        // with lcnSharing = true (via annotation), we allow more than one to be served.
        if (lcnSharing) {
            switch (ordering) {
                case SPECIFIED:
                    return super.getChannelsAvailable(date, lcnSharing);
                case CHANNEL_NUMBER:
                    return StreamSupport.stream(super.getChannelsAvailable(date, lcnSharing).spliterator(), false)
                            .sorted(CHANNEL_NUMBERING_ORDERING)
                            .collect(MoreCollectors.toImmutableSet());
                default:
                    throw new IllegalArgumentException("Unsupported channel ordering: " + ordering);
            }
        }

        Set<ChannelNumbering> deduplicatedNumberings = StreamSupport.stream(
                super.getChannelsAvailable(date, lcnSharing).spliterator(), false
        )
                //we need to use randomUUID in order to avoid deduplicating chanels which have no numbering
                .collect(Collectors.groupingBy(cn -> cn.getChannelNumber()
                        .orElse(UUID.randomUUID().toString())))
                .values()
                .stream()
                .map(
                        channelNumberings ->
                                channelNumberings.stream()
                                        .min(MOST_RECENT_START_DATE_COMPARATOR)
                                        .get()
                ).sorted(CHANNEL_NUMBERING_ORDERING).collect(MoreCollectors.toImmutableSet());

        switch (ordering) {
            case SPECIFIED:
                // N.B. we couldn't just return super.getChannelsAvailable(date, lcnSharing) here since that does not
                // actually deduplicate the channel numbers
                return StreamSupport.stream(
                        super.getChannelsAvailable(date, lcnSharing).spliterator(), false
                )
                        .filter(deduplicatedNumberings::contains) // this is using reference equality to work
                        .collect(MoreCollectors.toImmutableSet());
            case CHANNEL_NUMBER:
                return deduplicatedNumberings;
            default:
                throw new IllegalArgumentException("Unsupported channel ordering: " + ordering);
        }
    }
}
