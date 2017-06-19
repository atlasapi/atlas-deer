package org.atlasapi.query.v4.channel;

import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.ChannelMerger;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class MergingChannelWriter extends ChannelWriter {

    private final ChannelMerger merger;

    private MergingChannelWriter(
            String listName,
            String fieldName,
            ChannelGroupSummaryWriter channelGroupSummaryWriter,
            ChannelMerger merger
    ) {
        super(listName, fieldName, channelGroupSummaryWriter);

        this.merger = checkNotNull(merger);
    }

    public static MergingChannelWriter create(
            String listName,
            String fieldName,
            ChannelGroupSummaryWriter channelGroupSummaryWriter,
            ChannelMerger merger
    ) {
        return new MergingChannelWriter(listName, fieldName, channelGroupSummaryWriter, merger);
    }

    @Override
    public void write(
            ResolvedChannel entity,
            FieldWriter format,
            OutputContext ctxt
    ) throws IOException {
        if (entity.getEquivalents().isPresent()) {
            super.write(mergeEquivalents(entity, ctxt), format, ctxt);
        } else {
            super.write(entity, format, ctxt);
        }
    }

    private ResolvedChannel mergeEquivalents(ResolvedChannel resolvedChannel, OutputContext ctxt) {

        if (resolvedChannel.getEquivalents().isPresent()) {
            return ResolvedChannel.Builder.copyOf(resolvedChannel).withChannel(
                    merger.merge(
                            ctxt,
                            resolvedChannel.getChannel(),
                            resolvedChannel.getEquivalents().get()
                    )
            ).build();
        } else {
            return resolvedChannel;
        }

    }
}
