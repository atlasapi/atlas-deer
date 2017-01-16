package org.atlasapi.application;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.application.auth.InvalidApiKeyException;

import java.util.Optional;

public interface ApplicationFetcher {

    Optional<Application> applicationFor(HttpServletRequest request)
            throws InvalidApiKeyException;

    ImmutableSet<String> getParameterNames();

}
