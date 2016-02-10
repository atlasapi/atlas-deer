package org.atlasapi.query.v4.meta.endpoint;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

public class EndpointInfoQueryResultWriter extends QueryResultWriter<EndpointClassInfo> {

    private final EntityListWriter<EndpointClassInfo> modelListWriter;

    public EndpointInfoQueryResultWriter(
            EntityListWriter<EndpointClassInfo> modelListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.modelListWriter = modelListWriter;
    }

    @Override
    protected void writeResult(QueryResult<EndpointClassInfo> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<EndpointClassInfo> models = result.getResources();
            writer.writeList(modelListWriter, models, ctxt);
        } else {
            writer.writeObject(modelListWriter, result.getOnlyResource(), ctxt);
        }
    }

}
