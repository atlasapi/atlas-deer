package org.atlasapi.system.legacy;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public abstract class LegacyChannelGroupTransformer<F extends org.atlasapi.media.channel.ChannelGroup, T extends ChannelGroup> extends BaseLegacyResourceTransformer<F, T>{

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
}
