package org.atlasapi.query.common;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.InvalidApiKeyException;

public interface QueryParser<T> {

    Query<T> parse(HttpServletRequest request) throws QueryParseException, InvalidApiKeyException;
    
}
