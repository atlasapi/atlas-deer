package org.atlasapi.system.legacy;

import org.atlasapi.channel.Region;


public class LegacyRegionTransformer extends LegacyChannelGroupTransformer<org.atlasapi.media.channel.Region, Region>{


    @Override
    public Region apply(org.atlasapi.media.channel.Region input) {
        return Region.builder(input.getPublisher())
                .withAvailableCountries(input.getAvailableCountries())
                .withPlaformId(input.getPlatform())
                .withTitles(input.getAllTitles())
                .withChannels(transformChannelNumbering(input.getChannelNumberings(), input.getPublisher()))
                .build();
    }

}
