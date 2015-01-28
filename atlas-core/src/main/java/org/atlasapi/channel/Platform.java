package org.atlasapi.channel;

import com.metabroadcast.common.intl.Country;
import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class Platform extends ChannelGroup<ChannelNumbering> {

    private final Set<ChannelGroupRef> regions;

    public Platform(
            Publisher publisher,
            ChannelNumbering channels,
            Set<Country> availableCountries,
            Set<TemporalField<String>> titles,
            Set<ChannelGroupRef> regions
    ) {
        super(publisher, channels, availableCountries, titles);
        this.regions = regions;
    }

    public Set<ChannelGroupRef> getRegions() {
        return regions;
    }
}
