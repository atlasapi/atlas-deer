package org.atlasapi.output;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

public class ChannelMerger {

    private ChannelMerger() {}

    public static ChannelMerger create() {
        return new ChannelMerger();
    }

    public Channel merge(OutputContext ctxt, Channel channel, Iterable<Channel> equivalents) {

        Application application = ctxt.getApplication();

        if (!application.getConfiguration().isPrecedenceEnabled()) {
            return channel;
        }

        Map<Publisher, Channel> channelMap = Maps.newHashMap();

        channelMap.put(channel.getSource(), channel);

        StreamSupport.stream(equivalents.spliterator(), false)
                .filter(equivalentChannel -> application.getConfiguration()
                        .getEnabledReadSources()
                        .contains(equivalentChannel.getSource())
                )
                .forEach(equivalentChannel ->
                        channelMap.put(equivalentChannel.getSource(), equivalentChannel)
                );

        Channel.Builder mergedChannel = Channel.builderFrom(channel);

        ImmutableList<Publisher> orderedPublishers = getOrderedPublishers(application);

        mergeAdvertiseFrom(orderedPublishers, channelMap, mergedChannel);
        mergeAdvertiseTo(orderedPublishers, channelMap, mergedChannel);
        mergeAliases(orderedPublishers, channelMap, mergedChannel);

        return mergedChannel.build();
    }

    private ImmutableList<Publisher> getOrderedPublishers(Application application) {
        Ordering<Publisher> ordering = application.getConfiguration()
                .getReadPrecedenceOrdering();

        return application.getConfiguration()
                .getEnabledReadSources()
                .stream()
                .sorted(ordering)
                .collect(MoreCollectors.toImmutableList());

    }

    private void mergeAdvertiseFrom(
            List<Publisher> publishers,
            Map<Publisher, Channel> channelMap,
            Channel.Builder mergedChannel
    ) {
        for (Publisher publisher : publishers) {
            Channel channel = channelMap.get(publisher);
            if(channel != null && channel.getAdvertiseFrom() != null) {
                mergedChannel.withAdvertiseFrom(channel.getAdvertiseFrom());
                break;
            }
        }
    }

    private void mergeAdvertiseTo(
            List<Publisher> publishers,
            Map<Publisher, Channel> channelMap,
            Channel.Builder mergedChannel
    ) {
        for (Publisher publisher : publishers) {
            Channel channel = channelMap.get(publisher);
            if(channel != null && channel.getAdvertiseTo() != null) {
                mergedChannel.withAdvertiseTo(channel.getAdvertiseTo());
                break;
            }
        }
    }

    private void mergeAliases(
            List<Publisher> publishers,
            Map<Publisher, Channel> channelMap,
            Channel.Builder mergedChannel
    ) {
        for (Publisher publisher : publishers) {
            Channel channel = channelMap.get(publisher);
            if (channel != null && channel.getAliases() != null && !channel.getAliases().isEmpty()) {
                channel.getAliases().forEach(mergedChannel::withAlias);
            }
        }
    }
}
