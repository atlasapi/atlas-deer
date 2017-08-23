package org.atlasapi.output.annotation;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.content.BroadcastAggregator;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AggregatedBroadcastsAnnotation extends OutputAnnotation<Content, ResolvedContent> { //TODO: check this out

    private final BroadcastWriter broadcastWriter;
    private final BroadcastAggregator broadcastAggregator;
    private final NumberToShortStringCodec codec;

    private AggregatedBroadcastsAnnotation(
            BroadcastWriter broadcastWriter,
            BroadcastAggregator broadcastAggregator,
            NumberToShortStringCodec codec
    ) {
        this.broadcastWriter = broadcastWriter;
        this.broadcastAggregator = broadcastAggregator;
        this.codec = codec;
    }

    public static AggregatedBroadcastsAnnotation create(
            NumberToShortStringCodec codec,
            ChannelResolver channelResolver
    ) {
        return new AggregatedBroadcastsAnnotation(
                BroadcastWriter.create(
                        "aggregated_broadcasts",
                        "aggregated_broadcast",
                        codec
                ),
                BroadcastAggregator.create(channelResolver),
                codec
        );
    }

    @Override
    public void write(
            ResolvedContent entity,
            FieldWriter writer,
            OutputContext ctxt
    ) throws IOException {
        if(entity.getContent() instanceof Item) {
            writeAggregatedBroadcasts((Item)entity.getContent(), writer, ctxt);
        }
    }

    private void writeAggregatedBroadcasts(
            Item item,
            FieldWriter writer,
            OutputContext ctxt
    ) throws IOException {

        String downweighIds = ctxt.getRequest()
                .getParameter(Attributes.DOWNWEIGH.externalName());

        List<Id> downweighChannelIds;

        if (Strings.isNullOrEmpty(downweighIds)) {
            downweighChannelIds = ImmutableList.of();
        } else {
            downweighChannelIds = Arrays.stream(downweighIds.split(","))
                    .map(codec::decode)
                    .map(Id::valueOf)
                    .collect(MoreCollectors.toImmutableList());
        }

        writer.writeList(
                broadcastWriter,
                broadcastAggregator.aggregateBroadcasts(
                        item.getBroadcasts(),
                        ctxt.getPlatform(),
                        downweighChannelIds
                ),
                ctxt
        );

    }

}
