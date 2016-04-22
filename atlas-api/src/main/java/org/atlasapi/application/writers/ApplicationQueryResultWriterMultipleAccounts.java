package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.Application;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class ApplicationQueryResultWriterMultipleAccounts implements
        UserAccountsAwareQueryResultWriter<Application> {

    private final EntityListWriter<Application> applicationListWriter;

    public ApplicationQueryResultWriterMultipleAccounts(EntityListWriter<Application> applicationListWriter) {
        this.applicationListWriter = applicationListWriter;
    }

    @Override
    public void write(UserAccountsAwareQueryResult<Application> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAccountsAwareQueryResult<Application> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Application> resources = result.getResources();
            writer.writeList(applicationListWriter, resources, ctxt);
        } else {
            writer.writeObject(applicationListWriter, result.getOnlyResource(), ctxt);
        }

    }

    private OutputContext outputContext(UserAccountsAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}