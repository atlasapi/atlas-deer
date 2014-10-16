package org.atlasapi.query.v4.meta.endpoint;

import java.io.IOException;

import org.atlasapi.generation.model.EndpointClassInfo;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

public class EndpointInfoQueryResultWriter implements QueryResultWriter<EndpointClassInfo> {

    private final EntityListWriter<EndpointClassInfo> modelListWriter;
    
    public EndpointInfoQueryResultWriter(EntityListWriter<EndpointClassInfo> modelListWriter) {
        this.modelListWriter = modelListWriter;
    }

    @Override
    public void write(QueryResult<EndpointClassInfo> result, ResponseWriter writer) throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(QueryResult<EndpointClassInfo> result, ResponseWriter writer)
        throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<EndpointClassInfo> models = result.getResources();
            writer.writeList(modelListWriter, models, ctxt);
        } else {
            writer.writeObject(modelListWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
    
}
