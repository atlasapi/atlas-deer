package org.atlasapi.output;

import org.atlasapi.query.common.QueryResult;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>Writes out the result of a query, a {@link QueryResult} via the provided
 * {@link ResponseWriter}</p>
 * 
 * @param <T>
 */
public abstract class QueryResultWriter<T> {

    private final EntityWriter<Object> licenseWriter;
    private final EntityWriter<HttpServletRequest> requestWriter;
    protected QueryResultWriter(
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        this.licenseWriter = checkNotNull(licenseWriter);
        this.requestWriter = checkNotNull(requestWriter);
    }

    public void write(QueryResult<T> result, ResponseWriter responseWriter) throws IOException {
        OutputContext outputContext = OutputContext.valueOf(result.getContext());
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.writeObject(
                licenseWriter,
                licenseWriter.fieldName(result),
                result,
                outputContext
        );

        responseWriter.writeField("results", result.getTotalResults());

        responseWriter.writeObject(
                requestWriter,
                requestWriter.fieldName(outputContext.getRequest()),
                outputContext.getRequest(),
                outputContext
        );
        responseWriter.finishResponse();
    }

    protected abstract void writeResult(QueryResult<T> result, ResponseWriter writer) throws IOException;

}