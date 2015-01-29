package org.atlasapi.system.legacy;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;


public class LegacyPlatformTransformer extends LegacyChannelGroupTransformer<org.atlasapi.media.channel.Platform, Platform> {


    @Override
    public Platform apply(org.atlasapi.media.channel.Platform input) {
        return Platform.builder(input.getPublisher())
                .withAvailableCountries(input.getAvailableCountries())
                .withRegionIds(input.getRegions())
                .withTitles(input.getAllTitles())
                .withChannels(transformChannelNumbering(input.getChannelNumberings(), input.getPublisher()))
                .build();
    }


}
