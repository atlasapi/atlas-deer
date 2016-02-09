package org.atlasapi.application.auth;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.users.User;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public interface UserFetcher {

    Optional<UserRef> userRefFor(HttpServletRequest request);

    Optional<User> userFor(HttpServletRequest request);

    ImmutableSet<String> getParameterNames();

}
