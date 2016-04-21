package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.SourceLicense;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResult;
import org.atlasapi.output.useraware.UserAccountsAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class SourceLicenseQueryResultWriterMultipleAccounts implements UserAccountsAwareQueryResultWriter<SourceLicense> {

    private final EntityListWriter<SourceLicense> sourcesWriter;

    public SourceLicenseQueryResultWriterMultipleAccounts(EntityListWriter<SourceLicense> sourcesWriter) {
        this.sourcesWriter = sourcesWriter;
    }

    @Override
    public void write(UserAccountsAwareQueryResult<SourceLicense> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAccountsAwareQueryResult<SourceLicense> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<SourceLicense> resources = result.getResources();
            writer.writeList(sourcesWriter, resources, ctxt);
        } else {
            writer.writeObject(sourcesWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(UserAccountsAwareQueryContext
            queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}