package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class AuthProvidersListWriter implements EntityListWriter<OAuthProvider> {

    @Override
    public void write(OAuthProvider entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("namespace", entity.getNamespace().name().toLowerCase());
        writer.writeField("prompt", entity.getLoginPromptMessage());
        writer.writeField("authRequestUrl", entity.getAuthRequestUrl());
    }

    @Override
    public String fieldName(OAuthProvider entity) {
        return "auth_provider";
    }

    @Override
    public String listName() {
        return "auth_providers";
    }
}

