package org.atlasapi.query.common.validation;

import java.util.Collection;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.query.common.exceptions.InvalidParameterException;

import com.google.common.base.Joiner;

public abstract class AbstractRequestParameterValidator {

    protected static final Joiner commaJoiner = Joiner.on(", ");

    public HttpServletRequest validateParameters(HttpServletRequest request)
            throws InvalidParameterException {
        Set<String> requestParams = paramNames(request);

        Collection<String> invalidParams = determineInvalidParameters(requestParams);
        if (!invalidParams.isEmpty()) {
            throw new InvalidParameterException(invalidParameterMessage(invalidParams));
        }

        Collection<String> missingParams = determineMissingParameters(requestParams);
        if (!missingParams.isEmpty()) {
            throw new InvalidParameterException(missingParameterMessage(missingParams));
        }
        Collection<String> conflictingParams = determineConflictingParameters(requestParams);
        if (!conflictingParams.isEmpty()) {
            throw new InvalidParameterException(conflictingParameterMessage(conflictingParams));
        }
        if (!missingParams.isEmpty()) {
            throw new InvalidParameterException(conflictingParameterMessage(conflictingParams));
        }

        return request;
    }

    @SuppressWarnings("unchecked")
    private Set<String> paramNames(HttpServletRequest request) {
        return request.getParameterMap().keySet();
    }

    protected abstract Collection<String> determineInvalidParameters(Set<String> requestParams);

    protected abstract Collection<String> determineMissingParameters(Set<String> requestParams);

    protected abstract Collection<String> determineConflictingParameters(Set<String> requestParams);

    protected abstract String invalidParameterMessage(Collection<String> invalidParams);

    protected abstract String missingParameterMessage(Collection<String> missingParams);

    protected abstract String conflictingParameterMessage(Collection<String> conflictingParams);
}
