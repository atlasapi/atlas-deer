package org.atlasapi.users.videosource;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.users.videosource.model.VideoSourceOAuthProvider;

import com.google.common.collect.FluentIterable;

public class VideoSourceOAuthProvidersQueryResultWriter
        extends QueryResultWriter<VideoSourceOAuthProvider> {

    private final EntityListWriter<VideoSourceOAuthProvider> linkedServiceProvidersWriter;

    public VideoSourceOAuthProvidersQueryResultWriter(
            EntityListWriter<VideoSourceOAuthProvider> linkedServiceProvidersWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.linkedServiceProvidersWriter = linkedServiceProvidersWriter;
    }

    @Override
    protected void writeResult(QueryResult<VideoSourceOAuthProvider> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<VideoSourceOAuthProvider> resources = result.getResources();
            writer.writeList(linkedServiceProvidersWriter, resources, ctxt);
        } else {
            writer.writeObject(linkedServiceProvidersWriter, result.getOnlyResource(), ctxt);
        }
    }
}
