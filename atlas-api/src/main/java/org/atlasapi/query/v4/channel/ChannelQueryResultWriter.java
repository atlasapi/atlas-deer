package org.atlasapi.query.v4.channel;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryResultWriter extends QueryResultWriter<ResolvedChannel> {

    private final EntityListWriter<ResolvedChannel> channelListWriter;

    public ChannelQueryResultWriter(
            EntityListWriter<ResolvedChannel> channelListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.channelListWriter = checkNotNull(channelListWriter);
    }

    @Override
    protected void writeResult(QueryResult<ResolvedChannel> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            Iterable<ResolvedChannel> resources = result.getResources();

            writer.writeList(channelListWriter, resources, ctxt);
        } else {
            ResolvedChannel resolvedChannel = result.getOnlyResource();

            writer.writeObject(channelListWriter, resolvedChannel, ctxt);
        }

    }
}
