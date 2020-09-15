package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResolvedChannelResolver;
import org.atlasapi.output.writers.BroadcastWriter;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FirstBroadcastAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final ResolvedChannelResolver resolvedChannelResolver;

    private FirstBroadcastAnnotation(
            BroadcastWriter broadcastWriter,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        this.broadcastWriter = broadcastWriter;
        this.resolvedChannelResolver = resolvedChannelResolver;
    }

    public static FirstBroadcastAnnotation create(
            NumberToShortStringCodec codec,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        return new FirstBroadcastAnnotation(
                BroadcastWriter.create(
                        "first_broadcasts",
                        "broadcast",
                        codec
                ),
                resolvedChannelResolver
        );
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(FieldWriter writer, Item item, OutputContext ctxt)
            throws IOException {

        List<Broadcast> broadcasts = item.getBroadcasts().stream()
                .filter(Broadcast::isActivelyPublished)
                .collect(Collectors.toList());

        List<Broadcast> firstBroadcasts = ImmutableList.copyOf(firstBroadcasts(broadcasts));

        Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(firstBroadcasts);

        List<ResolvedBroadcast> resolvedBroadcasts = firstBroadcasts.stream()
                .map(broadcast -> ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId())))
                .collect(MoreCollectors.toImmutableList());

        writer.writeList(
                broadcastWriter,
                resolvedBroadcasts,
                ctxt
        );
    }

    private Iterable<Broadcast> firstBroadcasts(Iterable<Broadcast> broadcasts) {
        DateTime earliest = null;
        Builder<Broadcast> filteredBroadcasts = ImmutableSet.builder();
        for (Broadcast broadcast : broadcasts) {
            DateTime transmissionTime = broadcast.getTransmissionTime();
            if (earliest == null || transmissionTime.isBefore(earliest)) {
                earliest = transmissionTime;
                filteredBroadcasts = ImmutableSet.<Broadcast>builder().add(broadcast);
            } else if (transmissionTime.isEqual(earliest)) {
                filteredBroadcasts.add(broadcast);
            }
        }
        return filteredBroadcasts.build();
    }
}

