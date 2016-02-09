package org.atlasapi.query.common.useraware;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.InvalidApiKeyException;
import org.atlasapi.content.QueryParseException;

public interface UserAwareQueryParser<T> {

    UserAwareQuery<T> parse(HttpServletRequest request)
            throws QueryParseException, InvalidApiKeyException;

}
