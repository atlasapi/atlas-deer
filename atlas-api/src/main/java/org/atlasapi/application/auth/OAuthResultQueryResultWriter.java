package org.atlasapi.application.auth;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.model.auth.OAuthResult;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

public class OAuthResultQueryResultWriter extends QueryResultWriter<OAuthResult> {

    private final EntityListWriter<OAuthResult> oauthResultWriter;

    public OAuthResultQueryResultWriter(
            EntityListWriter<OAuthResult> oauthResultWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter
    ) {
        super(licenseWriter, requestWriter);
        this.oauthResultWriter = oauthResultWriter;
    }

    @Override
    protected void writeResult(QueryResult<OAuthResult> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<OAuthResult> resources = result.getResources();
            writer.writeList(oauthResultWriter, resources, ctxt);
        } else {
            writer.writeObject(oauthResultWriter, result.getOnlyResource(), ctxt);
        }
    }
}
