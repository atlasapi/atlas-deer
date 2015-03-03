package org.atlasapi.query.v4.channelgroup;

import com.google.common.collect.FluentIterable;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelGroupQueryResultWriter extends QueryResultWriter<ChannelGroup> {

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
    protected void writeResult(QueryResult<ChannelGroup> result, ResponseWriter writer) throws IOException {
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
