package org.atlasapi.application.auth;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;

import com.metabroadcast.common.social.auth.credentials.Credentials;
import com.metabroadcast.common.social.auth.credentials.CredentialsStore;
import com.metabroadcast.common.social.auth.facebook.AccessTokenChecker;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

public class OAuthTokenUserFetcher implements UserFetcher {

    public static final String OAUTH_PROVIDER_QUERY_PARAMETER = "oauth_provider";
    public static final String OAUTH_TOKEN_QUERY_PARAMETER = "oauth_token";

    private final CredentialsStore credentialsStore;
    private final Map<UserNamespace, AccessTokenChecker> accessTokenCheckers;
    private final UserStore userStore;

    public OAuthTokenUserFetcher(CredentialsStore credentialsStore,
            Map<UserNamespace, AccessTokenChecker> accessTokenCheckers,
            UserStore userStore) {
        this.credentialsStore = checkNotNull(credentialsStore);
        this.accessTokenCheckers = ImmutableMap.copyOf(accessTokenCheckers);
        this.userStore = checkNotNull(userStore);
    }

    public ImmutableSet<String> getParameterNames() {
        return ImmutableSet.of(
                OAUTH_PROVIDER_QUERY_PARAMETER,
                OAUTH_TOKEN_QUERY_PARAMETER
        );
    }

    public Optional<UserRef> userRefFor(HttpServletRequest request) {
        if (Strings.isNullOrEmpty(request.getParameter(OAUTH_PROVIDER_QUERY_PARAMETER))) {
            return Optional.absent();
        }

        UserNamespace oauthProviderNamespace = UserNamespace.valueOf(request.getParameter(
                OAUTH_PROVIDER_QUERY_PARAMETER).toUpperCase());
        String oauthToken = request.getParameter(OAUTH_TOKEN_QUERY_PARAMETER);
        Optional<Credentials> credentials = credentialsStore.credentialsForToken(
                oauthProviderNamespace,
                oauthToken
        );
        if (accessTokenCheckers.keySet().contains(oauthProviderNamespace)
                && credentials.isPresent()
                && accessTokenCheckers.get(oauthProviderNamespace)
                .check(credentials.get().authToken())
                .hasValue()) {
            return Optional.of(credentials.get().userRef());
        }

        return Optional.absent();
    }

    public Optional<User> userFor(HttpServletRequest request) {
        Optional<UserRef> userRef = userRefFor(request);
        if (userRef.isPresent()) {
            return userStore.userForRef(userRef.get());
        } else {
            return Optional.absent();
        }
    }
}
