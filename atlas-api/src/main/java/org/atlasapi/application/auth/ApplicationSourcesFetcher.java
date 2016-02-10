package org.atlasapi.application.auth;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.ApplicationSources;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public interface ApplicationSourcesFetcher {

    Optional<ApplicationSources> sourcesFor(HttpServletRequest request)
            throws InvalidApiKeyException;

    ImmutableSet<String> getParameterNames();

}
