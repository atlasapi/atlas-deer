package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthResult;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class OAuthResultListWriter implements EntityListWriter<OAuthResult> {

    @Override
    public void write(OAuthResult entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("success", entity.isSuccess());
        writer.writeField("provider", entity.getProvider().name().toLowerCase());
        writer.writeField("access_token", entity.getAccessToken());
    }

    @Override
    public String fieldName(OAuthResult entity) {
        return "oauth_result";
    }

    @Override
    public String listName() {
        return "oauth_results";
    }

}
