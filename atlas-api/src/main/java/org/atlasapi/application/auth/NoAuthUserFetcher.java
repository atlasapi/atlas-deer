package org.atlasapi.application.auth;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.auth.www.AuthController;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class NoAuthUserFetcher implements UserFetcher {

    public static final String EMAIL_PARAMETER = "email";

    private final UserStore userStore;

    public NoAuthUserFetcher(UserStore userStore) {
        this.userStore = userStore;
    }

    @Override
    public Optional<UserRef> userRefFor(HttpServletRequest request) {
        Optional<User> user = userFor(request);
        if (user.isPresent()) {
            return Optional.of(user.get().getUserRef());
        } else {
            return Optional.absent();
        }
    }

    @Override
    public Optional<User> userFor(HttpServletRequest request) {
        String email = request.getParameter(EMAIL_PARAMETER);
        Optional<User> user = userStore.userForEmail(email);
        if (user.isPresent()) {
            return user;
        } else {
            return Optional.absent();
        }
    }

    @Override
    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(EMAIL_PARAMETER);
    }
}
