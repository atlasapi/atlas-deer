package org.atlasapi.application.model.auth;

import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.collect.ImmutableSet;

public enum OAuthProvider {
    TWITTER(UserNamespace.TWITTER, "Sign in with Twitter", "/4/auth/twitter/login"),
    GITHUB(UserNamespace.GITHUB, "Sign in with GitHub", "/4/auth/github/login"),
    GOOGLE(UserNamespace.GOOGLE, "Sign in with Google", "/4/auth/google/login");

    private final UserNamespace namespace;
    private final String loginPromptMessage;
    private final String authRequestUrl;
    private static final ImmutableSet<OAuthProvider> ALL = ImmutableSet.copyOf(values());

    OAuthProvider(UserNamespace namespace, String loginPromptMessage, String authRequestUrl) {
        this.namespace = namespace;
        this.loginPromptMessage = loginPromptMessage;
        this.authRequestUrl = authRequestUrl;
    }

    public UserNamespace getNamespace() {
        return namespace;
    }

    public String getLoginPromptMessage() {
        return loginPromptMessage;
    }

    public String getAuthRequestUrl() {
        return authRequestUrl;
    }

    public static final ImmutableSet<OAuthProvider> all() {
        return ALL;
    }
}
