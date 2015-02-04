package org.atlasapi.system.legacy;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class LegacyChannelGroupTransformer extends BaseLegacyResourceTransformer<org.atlasapi.media.channel.ChannelGroup,ChannelGroup> {

    protected Iterable<ChannelNumbering> transformChannelNumbering(
            Set<org.atlasapi.media.channel.ChannelNumbering> channelNumberings,
            final Publisher publisher
    ) {
        return Iterables.transform(channelNumberings, new Function<org.atlasapi.media.channel.ChannelNumbering, ChannelNumbering>() {
            @Override
            public ChannelNumbering apply(org.atlasapi.media.channel.ChannelNumbering input) {
                return ChannelGroupMembership.builder(publisher)
                        .withChannelId(input.getChannel())
                        .withChannelGroupId(input.getChannelGroup())
                        .withChannelNumber(input.getChannelNumber())
                        .withStartDate(input.getStartDate())
                        .withEndDate(input.getEndDate())
                        .buildChannelNumbering();
            }
        });
    }



    @Override
    public ChannelGroup apply(org.atlasapi.media.channel.ChannelGroup input) {
        if(input instanceof org.atlasapi.media.channel.Platform) {
            return transformPlatform((org.atlasapi.media.channel.Platform) input);
        } else {
            return transformRegion((org.atlasapi.media.channel.Region) input);
        }
    }

    private Platform transformPlatform(org.atlasapi.media.channel.Platform input) {
        return Platform.builder(input.getPublisher())
                .withId(input.getId())
                .withAvailableCountries(input.getAvailableCountries())
                .withRegionIds(input.getRegions())
                .withTitles(input.getAllTitles())
                .withChannels(transformChannelNumbering(input.getChannelNumberings(), input.getPublisher()))
                .build();
    }

    public Region transformRegion(org.atlasapi.media.channel.Region input) {
        return Region.builder(input.getPublisher())
                .withId(input.getId())
                .withAvailableCountries(input.getAvailableCountries())
                .withPlaformId(input.getPlatform())
                .withTitles(input.getAllTitles())
                .withChannels(transformChannelNumbering(input.getChannelNumberings(), input.getPublisher()))
                .build();
    }
}
