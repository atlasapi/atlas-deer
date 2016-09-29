package org.atlasapi.query.v4.channelgroup;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.ResolvedChannelGroup;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryResultWriter extends QueryResultWriter<ResolvedChannelGroup> {

    private final ChannelGroupListWriter channelGroupListWriter;

    public ChannelGroupQueryResultWriter(
            ChannelGroupListWriter channelGroupListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.channelGroupListWriter = checkNotNull(channelGroupListWriter);
    }

    @Override
    protected void writeResult(QueryResult<ResolvedChannelGroup> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<ResolvedChannelGroup> resources = result.getResources();
            writer.writeList(channelGroupListWriter, resources, ctxt);
        } else {
            writer.writeObject(channelGroupListWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
