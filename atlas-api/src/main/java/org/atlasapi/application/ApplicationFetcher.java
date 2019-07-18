package org.atlasapi.application;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.applications.client.model.internal.Application;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

public interface ApplicationFetcher {

    Optional<Application> applicationFor(HttpServletRequest request)
            throws ApplicationResolutionException;

    Optional<Application> applicationForApiKey(String apiKey)
            throws ApplicationResolutionException;

    ImmutableSet<String> getParameterNames();

}
