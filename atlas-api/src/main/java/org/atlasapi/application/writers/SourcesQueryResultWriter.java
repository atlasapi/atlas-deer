package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class SourcesQueryResultWriter implements UserAwareQueryResultWriter<Publisher> {

    private final EntityListWriter<Publisher> sourcesWriter;

    public SourcesQueryResultWriter(EntityListWriter<Publisher> sourcesWriter) {
        this.sourcesWriter = sourcesWriter;
    }

    @Override
    public void write(UserAwareQueryResult<Publisher> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAwareQueryResult<Publisher> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Publisher> resources = result.getResources();
            writer.writeList(sourcesWriter, resources, ctxt);
        } else {
            writer.writeObject(sourcesWriter, result.getOnlyResource(), ctxt);
        }

    }

    private OutputContext outputContext(UserAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
