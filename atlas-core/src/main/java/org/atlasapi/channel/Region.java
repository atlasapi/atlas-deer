package org.atlasapi.channel;

import com.metabroadcast.common.intl.Country;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class Region extends ChannelGroup<ChannelNumbering> {

    private final ChannelGroupRef platform;

    public Region(
            Publisher publisher,
            ChannelNumbering channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            ChannelGroupRef platform
    ) {
        super(publisher, channels, availableCountries, titles);
        this.platform = platform;
    }

    public ChannelGroupRef getPlatform() {
        return platform;
    }
}
