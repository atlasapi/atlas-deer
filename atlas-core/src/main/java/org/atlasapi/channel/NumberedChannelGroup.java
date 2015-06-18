package org.atlasapi.channel;

import com.metabroadcast.common.intl.Country;
import org.atlasapi.entity.Id;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class NumberedChannelGroup extends ChannelGroup<ChannelNumbering> {

    private static final ChannelNumberingOrdering CHANNEL_NUMBERING_ORDERING = new ChannelNumberingOrdering();

    protected NumberedChannelGroup(Id id, Publisher publisher, Set<ChannelNumbering> channels, Set<Country> availableCountries, Set<TemporalField<String>> titles) {
        super(id, publisher, channels, availableCountries, titles);
    }


    @Override
    public Iterable<ChannelNumbering> getChannels() {
        return StreamSupport.stream(super.getChannels().spliterator(), false)
                .sorted(CHANNEL_NUMBERING_ORDERING)
                .collect(Collectors.toList());
    }
}
