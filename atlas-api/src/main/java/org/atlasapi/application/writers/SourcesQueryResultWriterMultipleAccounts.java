package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class SourcesQueryResultWriterMultipleAccounts implements
        UserAccountsAwareQueryResultWriter<Publisher> {

    private final EntityListWriter<Publisher> sourcesWriter;

    public SourcesQueryResultWriterMultipleAccounts(EntityListWriter<Publisher> sourcesWriter) {
        this.sourcesWriter = sourcesWriter;
    }

    @Override
    public void write(UserAccountsAwareQueryResult<Publisher> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAccountsAwareQueryResult<Publisher> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Publisher> resources = result.getResources();
            writer.writeList(sourcesWriter, resources, ctxt);
        } else {
            writer.writeObject(sourcesWriter, result.getOnlyResource(), ctxt);
        }

    }

    private OutputContext outputContext(UserAccountsAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
