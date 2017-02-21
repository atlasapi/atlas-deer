package org.atlasapi.output.annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.BroadcastAggregator;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AggregatedBroadcastWriter;

import java.io.IOException;

public class AggregatedBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final AggregatedBroadcastWriter aggregatedBroadcastWriter;
    private final BroadcastAggregator broadcastAggregator;

    private AggregatedBroadcastsAnnotation(
            AggregatedBroadcastWriter aggregatedBroadcastWriter,
            BroadcastAggregator broadcastAggregator
    ) {
        this.aggregatedBroadcastWriter = aggregatedBroadcastWriter;
        this.broadcastAggregator = broadcastAggregator;
    }

    public static AggregatedBroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new AggregatedBroadcastsAnnotation(
                AggregatedBroadcastWriter.create(codec),
                BroadcastAggregator.create(channelResolver)
        );
    }

    @Override
    public void write(
            Content entity,
            FieldWriter writer,
            OutputContext ctxt
    ) throws IOException {
        if(entity instanceof Item) {
            writeAggregatedBroadcasts((Item)entity, writer, ctxt);
        }
    }

    private void writeAggregatedBroadcasts(
            Item item,
            FieldWriter writer,
            OutputContext ctxt
    ) throws IOException {
        writer.writeList(
                aggregatedBroadcastWriter,
                broadcastAggregator.aggregateBroadcasts(item.getBroadcasts(), ctxt.getPlatform()),
                ctxt
        );

    }

}
