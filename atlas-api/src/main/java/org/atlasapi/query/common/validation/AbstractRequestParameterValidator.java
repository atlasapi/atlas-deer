package org.atlasapi.query.common.validation;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.query.common.exceptions.InvalidParameterException;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.Set;

public abstract class AbstractRequestParameterValidator {

    // This attribute is used only to prevent browsers from using a cached result for the same request
    public static final String T_PARAM = "t";

    public static final Set<String> ALWAYS_OPTIONAL_PARAMS = ImmutableSet.of(T_PARAM);

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
