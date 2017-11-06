package org.atlasapi.output.annotation;

import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelEquivRef;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.OutputContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class FullyResolvingChannelGroupMerger {

    private final ChannelResolver channelResolver;

    private FullyResolvingChannelGroupMerger(
            ChannelResolver channelResolver
    ) {
        this.channelResolver = checkNotNull(channelResolver);
    }

    public ImmutableSet<ChannelGroupMembership> resolveAndMergeChannelGroups(
            OutputContext ctxt,
            Channel channel
    ) {
        List<Channel> equivChannels = resolveChannelEquivs(channel, getOrderedPublishers(ctxt));

        Set<ChannelGroupMembership> mergedMemberships = Sets.newHashSet();

        for (Channel equivChannel : equivChannels) {
            equivChannel.getChannelGroups().forEach(membership -> {
                if (!mergedMemberships.contains(membership)) {
                    mergedMemberships.add(membership);
                }
            });
        }

        return ImmutableSet.copyOf(mergedMemberships);
    }

    private List<Publisher> getOrderedPublishers(OutputContext ctxt) {
        return ctxt.getApplication().getConfiguration().getReadPrecedenceOrdering()
                .sortedCopy(ctxt.getApplication().getConfiguration().getEnabledReadSources());
    }

    private List<Channel> resolveChannelEquivs(Channel channel, List<Publisher> publishers) {
        try {
            List<Channel> resolvedChannels = channelResolver.resolveIds(
                    Iterables.transform(channel.getSameAs(), ChannelEquivRef::getId)
            )
                    .get()
                    .getResources()
                    .toList();

            return resolvedChannels.stream()
                    .filter(channel1 -> publishers.contains(channel1.getSource()))
                    .collect(Collectors.toList());

        } catch (Exception e) {
            return ImmutableList.of();
        }
    }

}
