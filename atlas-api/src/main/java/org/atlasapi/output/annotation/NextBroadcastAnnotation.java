package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

public class NextBroadcastAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final Clock clock;

    public NextBroadcastAnnotation(
            Clock clock,
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver) {
        super();
        this.clock = clock;
        this.broadcastWriter = new BroadcastWriter(
                "next_broadcasts",
                codec,
                channelResolver
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
        writer.writeList(
                broadcastWriter,
                nextBroadcast(Iterables.filter(item.getBroadcasts(), Broadcast::isActivelyPublished)),
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
