package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.SourceRequest;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class SourcesRequestsQueryResultsWriter implements
        UserAccountsAwareQueryResultWriter<SourceRequest> {

    private final EntityListWriter<SourceRequest> sourcesRequestListWriter;

    public SourcesRequestsQueryResultsWriter(
            EntityListWriter<SourceRequest> sourcesRequestListWriter) {
        this.sourcesRequestListWriter = sourcesRequestListWriter;
    }

    @Override
    public void write(UserAccountsAwareQueryResult<SourceRequest> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAccountsAwareQueryResult<SourceRequest> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<SourceRequest> resources = result.getResources();
            writer.writeList(sourcesRequestListWriter, resources, ctxt);
        } else {
            writer.writeObject(sourcesRequestListWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(UserAccountsAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
