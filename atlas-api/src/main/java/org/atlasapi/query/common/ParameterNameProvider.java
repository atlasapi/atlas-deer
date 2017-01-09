package org.atlasapi.query.common;

import java.util.Set;

public interface ParameterNameProvider {

    Set<String> getRequiredParameters();

    Set<String> getOptionalParameters();

}
