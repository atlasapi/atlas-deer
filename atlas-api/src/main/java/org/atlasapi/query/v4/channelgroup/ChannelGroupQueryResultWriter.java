package org.atlasapi.query.v4.channelgroup;

import com.google.common.collect.FluentIterable;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryResultWriter implements QueryResultWriter<ChannelGroup> {

    private final ChannelGroupListWriter channelGroupListWriter;

    public ChannelGroupQueryResultWriter(ChannelGroupListWriter channelGroupListWriter) {
        this.channelGroupListWriter = checkNotNull(channelGroupListWriter);
    }

    @Override
    public void write(QueryResult<ChannelGroup> result, ResponseWriter writer) throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(QueryResult<ChannelGroup> result, ResponseWriter writer) throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<ChannelGroup> resources = result.getResources();
            writer.writeList(channelGroupListWriter, resources, ctxt);
        } else {
            writer.writeObject(channelGroupListWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
