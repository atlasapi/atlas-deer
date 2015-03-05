package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

import javax.servlet.http.HttpServletRequest;

public class OAuthRequestQueryResultWriter extends QueryResultWriter<OAuthRequest> {
    private final EntityListWriter<OAuthRequest> oauthRequestWriter;

    public OAuthRequestQueryResultWriter(
            EntityListWriter<OAuthRequest> oauthRequestWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.oauthRequestWriter = oauthRequestWriter;
    }
    
    @Override
    protected void writeResult(QueryResult<OAuthRequest> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<OAuthRequest> resources = result.getResources();
            writer.writeList(oauthRequestWriter, resources, ctxt);
        } else {
            writer.writeObject(oauthRequestWriter, result.getOnlyResource(), ctxt);
        }
    }

}
