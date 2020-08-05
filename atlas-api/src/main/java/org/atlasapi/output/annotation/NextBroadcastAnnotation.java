package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NextBroadcastAnnotation extends OutputAnnotation<Content> {

    private static final Logger log = LoggerFactory.getLogger(NextBroadcastAnnotation.class);
    private final BroadcastWriter broadcastWriter;
    private final Clock clock;
    private final ResolvedChannelResolver resolvedChannelResolver;

    private NextBroadcastAnnotation(
            Clock clock,
            BroadcastWriter broadcastWriter,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        super();
        this.clock = clock;
        this.broadcastWriter = broadcastWriter;
        this.resolvedChannelResolver = resolvedChannelResolver;
    }

    public static NextBroadcastAnnotation create(
            Clock clock,
            NumberToShortStringCodec codec,
            ResolvedChannelResolver resolvedChannelResolver
    ) {
        return new NextBroadcastAnnotation(
                clock,
                BroadcastWriter.create(
                        "next_broadcasts",
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
                .collect(MoreCollectors.toImmutableList());

        List<Broadcast> nextBroadcasts = ImmutableList.copyOf(nextBroadcast(broadcasts));

        Map<Id, ResolvedChannel> channelMap = resolvedChannelResolver.resolveChannelMap(nextBroadcasts);

        List<ResolvedBroadcast> resolvedBroadcasts = nextBroadcasts.stream()
                .map(broadcast -> ResolvedBroadcast.create(broadcast, channelMap.get(broadcast.getChannelId())))
                .collect(MoreCollectors.toImmutableList());

        writer.writeList(
                broadcastWriter,
                resolvedBroadcasts,
                ctxt
        );
    }

    private Iterable<Broadcast> nextBroadcast(Iterable<Broadcast> broadcasts) {
        DateTime now = clock.now();
        DateTime earliest = null;
        Builder<Broadcast> filteredBroadcasts = ImmutableSet.builder();
        for (Broadcast broadcast : broadcasts) {
            DateTime transmissionTime = broadcast.getTransmissionTime();
            if (transmissionTime.isAfter(now) && (earliest == null || transmissionTime.isBefore(
                    earliest))) {
                earliest = transmissionTime;
                filteredBroadcasts = ImmutableSet.<Broadcast>builder().add(broadcast);
            } else if (transmissionTime.isEqual(earliest)) {
                filteredBroadcasts.add(broadcast);
            }
        }
        return filteredBroadcasts.build();
    }
}
