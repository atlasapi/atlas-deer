package org.atlasapi.application;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;

public interface ApplicationFetcher {

    Optional<Application> applicationFor(HttpServletRequest request)
            throws ApplicationResolutionException;

    ImmutableSet<String> getParameterNames();

}