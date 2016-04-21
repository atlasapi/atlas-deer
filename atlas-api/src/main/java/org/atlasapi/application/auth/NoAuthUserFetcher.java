package org.atlasapi.application.auth;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class NoAuthUserFetcher {

    public static final String EMAIL_PARAMETER = "email";

    private final UserStore userStore;

    public NoAuthUserFetcher(UserStore userStore) {
        this.userStore = userStore;
    }

    public Set<User> userFor(HttpServletRequest request) {
        String email = request.getParameter(EMAIL_PARAMETER);
        Set<User> userAccounts = userStore.userAccountsForEmail(email);
        return userAccounts;
    }

    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(EMAIL_PARAMETER);
    }
}
