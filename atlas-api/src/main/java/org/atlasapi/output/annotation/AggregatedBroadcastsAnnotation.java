package org.atlasapi.output.annotation;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.BroadcastAggregator;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AggregatedBroadcastWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AggregatedBroadcastsAnnotation extends OutputAnnotation<Content> {

    private final AggregatedBroadcastWriter aggregatedBroadcastWriter;
    private final BroadcastAggregator broadcastAggregator;
    private final NumberToShortStringCodec codec;

    private AggregatedBroadcastsAnnotation(
            AggregatedBroadcastWriter aggregatedBroadcastWriter,
            BroadcastAggregator broadcastAggregator,
            NumberToShortStringCodec codec
    ) {
        this.aggregatedBroadcastWriter = aggregatedBroadcastWriter;
        this.broadcastAggregator = broadcastAggregator;
        this.codec = codec;
    }

    public static AggregatedBroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new AggregatedBroadcastsAnnotation(
                AggregatedBroadcastWriter.create(codec),
                BroadcastAggregator.create(channelResolver)
                codec
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
        List<Id> downweighChannelIds = Arrays.stream(
                ctxt.getRequest()
                        .getParameter(Attributes.DOWNWEIGH.externalName())
                        .split(","))
                .map(codec::decode)
                .map(Id::valueOf)
                .collect(MoreCollectors.toImmutableList());

        writer.writeList(
                aggregatedBroadcastWriter,
                broadcastAggregator.aggregateBroadcasts(
                        item.getBroadcasts(),
                        ctxt.getPlatform(),
                        downweighChannelIds
                ),
                ctxt
        );

    }

}
