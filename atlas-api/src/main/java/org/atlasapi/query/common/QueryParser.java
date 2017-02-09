package org.atlasapi.query.common;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationResolutionException;
import org.atlasapi.content.QueryParseException;

public interface QueryParser<T> {

    Query<T> parse(HttpServletRequest request) throws QueryParseException,
            ApplicationResolutionException;

}
