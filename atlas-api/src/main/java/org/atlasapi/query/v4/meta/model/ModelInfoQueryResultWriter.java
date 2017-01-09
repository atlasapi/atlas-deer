package org.atlasapi.query.v4.meta.model;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.generation.model.ModelClassInfo;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;

import com.google.common.collect.FluentIterable;

public class ModelInfoQueryResultWriter extends QueryResultWriter<ModelClassInfo> {

    private final EntityListWriter<ModelClassInfo> modelListWriter;

    public ModelInfoQueryResultWriter(
            EntityListWriter<ModelClassInfo> modelListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.modelListWriter = modelListWriter;
    }

    @Override
    protected void writeResult(QueryResult<ModelClassInfo> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<ModelClassInfo> models = result.getResources();
            writer.writeList(modelListWriter, models, ctxt);
        } else {
            writer.writeObject(modelListWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
