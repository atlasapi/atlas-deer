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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class NumberedChannelGroup extends ChannelGroup<ChannelNumbering> {

    private static final ChannelNumberingOrdering CHANNEL_NUMBERING_ORDERING = new ChannelNumberingOrdering();

    private static final LocalDate EARLIEST_POSSIBLE_DATE = new LocalDate(0, 1, 1);

    private static final Comparator<ChannelNumbering> MOST_RECENT_START_DATE_COMPARATOR =
            (channelNumbering1, channelNumbering2) ->
                    -channelNumbering1.getStartDate().orElse(EARLIEST_POSSIBLE_DATE)
                            .compareTo(
                                    channelNumbering2.getStartDate().orElse(EARLIEST_POSSIBLE_DATE)
                            );

    protected final ChannelGroupRef channelNumbersFrom;

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
            Iterable<ChannelNumbering> channels,
            Iterable<Country> availableCountries,
            Iterable<TemporalField<String>> titles,
            @Nullable ChannelGroupRef channelNumbersFrom
    ) {
        super(id, publisher, channels, availableCountries, titles);
        this.channelNumbersFrom = channelNumbersFrom;
    }

    protected NumberedChannelGroup(
            Id id,
            String canonicalUri,
            Publisher publisher,
            Iterable<ChannelNumbering> channels,
            Iterable<Country> availableCountries,
            Iterable<TemporalField<String>> titles,
            @Nullable ChannelGroupRef channelNumbersFrom
    ) {
        super(id, canonicalUri, publisher, channels, availableCountries, titles);
        this.channelNumbersFrom = channelNumbersFrom;
    }

    public Optional<ChannelGroupRef> getChannelNumbersFrom() {
        return Optional.ofNullable(channelNumbersFrom);
    }

    @Override
    public Set<ChannelNumbering> getChannels() {
        return getChannels(ChannelOrdering.CHANNEL_NUMBER);
    }

    public Set<ChannelNumbering> getChannels(ChannelOrdering ordering) {
        switch (ordering) {
            case SPECIFIED:
                return super.getChannels();
            case CHANNEL_NUMBER:
                return super.getChannels().stream()
                        .sorted(CHANNEL_NUMBERING_ORDERING)
                        .collect(MoreCollectors.toImmutableSet());
            default:
                throw new IllegalArgumentException("Unsupported channel ordering: " + ordering);
        }
    }

    @Override
    public Set<ChannelNumbering> getChannelsAvailable(LocalDate date) {
        return getChannelsAvailable(date, ChannelOrdering.CHANNEL_NUMBER, false);
    }

    public Set<ChannelNumbering> getChannelsAvailable(LocalDate date, ChannelOrdering ordering) {
        return getChannelsAvailable(date, ordering, false);
    }

    public Set<ChannelNumbering> getChannelsAvailable(LocalDate date, boolean lcnSharing) {
        return getChannelsAvailable(date, ChannelOrdering.CHANNEL_NUMBER, lcnSharing);
    }

    public Set<ChannelNumbering> getChannelsAvailable(
            LocalDate date,
            ChannelOrdering ordering,
            boolean lcnSharing
    ) {
        // normally within a channel group, we expect/want only channel per channel number AKA lcn.
        // with lcnSharing = true (via annotation), we allow more than one to be served.
        if (lcnSharing) {
            switch (ordering) {
                case SPECIFIED:
                    return super.getChannelsAvailable(date);
                case CHANNEL_NUMBER:
                    return super.getChannelsAvailable(date).stream()
                            .sorted(CHANNEL_NUMBERING_ORDERING)
                            .collect(MoreCollectors.toImmutableSet());
                default:
                    throw new IllegalArgumentException("Unsupported channel ordering: " + ordering);
            }
        }


        Set<ChannelNumbering> deduplicatedChannelNumberingsWithChannelNumber = super.getChannelsAvailable(date).stream()
                .filter(channelNumbering -> channelNumbering.getChannelNumber().isPresent())
                .collect(Collectors.groupingBy(channelNumbering -> channelNumbering.getChannelNumber().get()))
                .values()
                .stream()
                .map(channelNumberingsForChannelNumber -> channelNumberingsForChannelNumber.stream()
                        .min(MOST_RECENT_START_DATE_COMPARATOR).get()
                )
                .collect(MoreCollectors.toImmutableSet());

        Stream<ChannelNumbering> deduplicatedChannelNumberings = super.getChannelsAvailable(date).stream()
                .filter(channelNumbering -> !channelNumbering.getChannelNumber().isPresent()
                        // this is using reference equality to work
                        || deduplicatedChannelNumberingsWithChannelNumber.contains(channelNumbering)
                );

        switch (ordering) {
            case SPECIFIED:
                // N.B. we couldn't just return super.getChannelsAvailable(date, lcnSharing) here since that does not
                // actually deduplicate the channel numbers
                return deduplicatedChannelNumberings.collect(MoreCollectors.toImmutableSet());
            case CHANNEL_NUMBER:
                return deduplicatedChannelNumberings.sorted(CHANNEL_NUMBERING_ORDERING)
                        .collect(MoreCollectors.toImmutableSet());
            default:
                throw new IllegalArgumentException("Unsupported channel ordering: " + ordering);
        }
    }
}
