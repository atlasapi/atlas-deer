package org.atlasapi.query.common.useraware;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.ApiKeyNotFoundException;
import org.atlasapi.application.auth.RevokedApiKeyException;
import org.atlasapi.query.common.QueryParseException;

public interface UserAwareQueryParser<T> {

    UserAwareQuery<T> parse(HttpServletRequest request) throws QueryParseException, RevokedApiKeyException, ApiKeyNotFoundException;
    
}
