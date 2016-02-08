package org.atlasapi.query.v2;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import com.google.common.collect.ImmutableSet;

public class ParameterChecker {

    private final Set<String> validParameters;

    public ParameterChecker(Iterable<String> validParameters) {
        this.validParameters = ImmutableSet.copyOf(validParameters);
    }

    public void checkParameters(HttpServletRequest request) {
        @SuppressWarnings("unchecked")
        Set<String> parameters = request.getParameterMap().keySet();

        for (String parameter : parameters) {
            if (!validParameters.contains(parameter)) {
                throw new IllegalArgumentException("Parameter "
                        + parameter
                        + " not applicable for this endpoint");
            }
        }
    }
}
