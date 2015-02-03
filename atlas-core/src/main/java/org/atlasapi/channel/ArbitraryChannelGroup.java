package org.atlasapi.channel;

import com.metabroadcast.common.intl.Country;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class ArbitraryChannelGroup extends ChannelGroup<ChannelGroupMembership> {

    public ArbitraryChannelGroup(
            Publisher publisher,
            Set<ChannelGroupMembership> channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles
    ) {
        super(publisher, channels, availableCountries, titles);
    }
}
