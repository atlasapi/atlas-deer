package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

import javax.servlet.http.HttpServletRequest;

public class AuthProvidersQueryResultWriter extends QueryResultWriter<OAuthProvider> {
    private final EntityListWriter<OAuthProvider> authProvidersWriter;
    
    public AuthProvidersQueryResultWriter(
            EntityListWriter<OAuthProvider> authProvidersWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.authProvidersWriter = authProvidersWriter;
    }
    
    @Override
    protected void writeResult(QueryResult<OAuthProvider> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<OAuthProvider> resources = result.getResources();
            writer.writeList(authProvidersWriter, resources, ctxt);
        } else {
            writer.writeObject(authProvidersWriter, result.getOnlyResource(), ctxt);
        }
    }
}
