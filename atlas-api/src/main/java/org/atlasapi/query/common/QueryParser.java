package org.atlasapi.query.common;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.ApiKeyNotFoundException;
import org.atlasapi.application.auth.RevokedApiKeyException;

public interface QueryParser<T> {

    Query<T> parse(HttpServletRequest request) throws QueryParseException, RevokedApiKeyException, ApiKeyNotFoundException;
    
}
