package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.SourceLicense;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class SourceLicenseQueryResultWriter implements UserAwareQueryResultWriter<SourceLicense> {

    private final EntityListWriter<SourceLicense> sourcesWriter;

    public SourceLicenseQueryResultWriter(EntityListWriter<SourceLicense> sourcesWriter) {
        this.sourcesWriter = sourcesWriter;
    }

    @Override
    public void write(UserAwareQueryResult<SourceLicense> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAwareQueryResult<SourceLicense> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<SourceLicense> resources = result.getResources();
            writer.writeList(sourcesWriter, resources, ctxt);
        } else {
            writer.writeObject(sourcesWriter, result.getOnlyResource(), ctxt);
        }
    }

    private OutputContext outputContext(UserAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
