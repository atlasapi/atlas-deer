package org.atlasapi.query.v4.channel;

import com.google.common.collect.FluentIterable;
import org.atlasapi.channel.Channel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryResultWriter extends QueryResultWriter<Channel> {

    private final EntityListWriter<Channel> channelListWriter;

    public ChannelQueryResultWriter(
            EntityListWriter<Channel> channelListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.channelListWriter = checkNotNull(channelListWriter);
    }

    @Override
    protected void writeResult(QueryResult<Channel> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Channel> resources = result.getResources();
            writer.writeList(channelListWriter, resources, ctxt);
        } else {
            writer.writeObject(channelListWriter, result.getOnlyResource(), ctxt);
        }

    }
}
