package org.atlasapi.query.v4.channel;

import com.google.common.collect.FluentIterable;
import org.atlasapi.channel.Channel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryResultWriter implements QueryResultWriter<Channel> {

    private final EntityListWriter<Channel> channelListWriter;

    public ChannelQueryResultWriter(EntityListWriter<Channel> channelListWriter) {
        this.channelListWriter = checkNotNull(channelListWriter);
    }

    @Override
    public void write(QueryResult<Channel> result, ResponseWriter writer)
            throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(QueryResult<Channel> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Channel> resources = result.getResources();
            writer.writeList(channelListWriter, resources, ctxt);
        } else {
            writer.writeObject(channelListWriter, result.getOnlyResource(), ctxt);
        }

    }

    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
